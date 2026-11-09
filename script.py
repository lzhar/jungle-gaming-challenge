#!/usr/bin/env python3
"""
Executa todo o roteiro de testes do Distributed Wagering Processor e grava o
resultado em um arquivo .txt (padrao: resultados_testes.txt).

Uso (rode DENTRO da pasta do projeto, onde esta o docker-compose.yml):

  python3 script.py                  # a API ja esta rodando (bun start)
  python3 script.py --setup          # sobe Docker, migrations, filas e a API sozinho
  python3 script.py --setup --failures --crash   # inclui os testes destrutivos

Opcoes:
  --base URL        endereco da API (padrao http://localhost:3000)
  --bases A,B,C     varias instancias para o teste de concorrencia K3
  --out ARQUIVO     arquivo de saida (padrao resultados_testes.txt)
  --setup           sobe infraestrutura + migrations + filas + API e encerra a API no fim
  --failures        testes F2/F3: derruba Postgres e LocalStack e religa
  --crash           teste S8: processo morre depois do commit e antes do ack (exige --setup)

Requisitos: Python 3.8+, Docker, Bun. Nao usa bibliotecas externas.
"""
import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --------------------------------------------------------------------------- args
ap = argparse.ArgumentParser(description='Roteiro de testes do Distributed Wagering Processor')
ap.add_argument('--project', default=os.getcwd(), help='pasta do projeto (padrao: pasta atual)')
ap.add_argument('--base', default='http://localhost:3000')
ap.add_argument('--bases', default='')
ap.add_argument('--out', default='resultados_testes.txt')
ap.add_argument('--setup', action='store_true')
ap.add_argument('--failures', action='store_true')
ap.add_argument('--crash', action='store_true')
args = ap.parse_args()

PROJECT = os.path.abspath(args.project)
BASE = args.base.rstrip('/')
BASES = [b.strip().rstrip('/') for b in args.bases.split(',') if b.strip()] or [BASE]
RUN = str(int(time.time()))
OUT = args.out if os.path.isabs(args.out) else os.path.join(PROJECT, args.out)
API_LOG = os.path.join(PROJECT, 'api.log')

RESULTS = []
S = {}          # estado compartilhado entre os testes (ids das wallets etc.)
API_PROC = {'p': None}


# --------------------------------------------------------------------------- utilitarios
def uuid7():
    ts = int(time.time() * 1000).to_bytes(6, 'big')
    b = bytearray(ts + os.urandom(10))
    b[6] = (b[6] & 0x0F) | 0x70
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


class Resp:
    def __init__(self, status, json_body, text, headers):
        self.status = status
        self.json = json_body
        self.text = text
        self.headers = headers

    def get(self, key, default=None):
        return (self.json or {}).get(key, default) if isinstance(self.json, dict) else default


def http(method, path, body=None, headers=None, base=None):
    url = (base or BASE) + path
    data = None
    h = dict(headers or {})
    if body is not None:
        data = json.dumps(body).encode()
        h['content-type'] = 'application/json'
    elif method == 'POST':
        data = b''
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            status, raw, hdrs = r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        status, raw, hdrs = e.code, e.read(), dict(e.headers)
    text = raw.decode(errors='replace')
    try:
        js = json.loads(text)
    except Exception:
        js = None
    return Resp(status, js, text, hdrs)


def run(cmd, **kw):
    return subprocess.run(cmd, cwd=PROJECT, capture_output=True, text=True, **kw)


def sql(query):
    p = run(['docker', 'compose', 'exec', '-T', 'postgres', 'psql', '-U', 'wager', '-d', 'wager', '-At', '-c', query])
    return p.returncode, (p.stdout + p.stderr).strip()


def sql_val(query):
    return sql(query)[1]


def wait_until(fn, timeout=30, every=1.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        time.sleep(every)
    return None


def metric(name, label=None, base=None):
    txt = http('GET', '/metrics', base=base).text
    pat = re.escape(name) + (re.escape('{' + label + '}') if label else '') + r'\s+([0-9.eE+-]+)\s*$'
    m = re.search('^' + pat, txt, re.M)
    return float(m.group(1)) if m else 0.0


def money(r, key='balance'):
    v = r.get(key)
    return v.get('amount') if isinstance(v, dict) else None


def record(tid, title, expected, actual, ok, status=None):
    st = status or ('PASS' if ok else 'FAIL')
    RESULTS.append({'id': tid, 'title': title, 'expected': expected, 'actual': actual, 'status': st})
    print('[%s] %s - %s' % (st, tid, title), flush=True)


def check(tid, title, expected, actual, ok):
    record(tid, title, expected, actual, bool(ok))


def skip(tid, title, reason):
    record(tid, title, '', reason, False, status='SKIP')


def safe(fn):
    try:
        fn()
    except Exception:
        record('ERRO', fn.__name__, 'executar sem excecao', traceback.format_exc(limit=3).strip(), False)


# --------------------------------------------------------------------------- operacoes da API
def new_wallet(amount, player=None):
    player = player or uuid7()
    r = http('POST', '/wallets', {'playerId': player, 'initialBalance': {'amount': amount, 'currency': 'BRL'}})
    return player, r


def wager(wallet, player, ext, kind, amount, ref=None, currency='BRL', base=None, key=None, no_header=False):
    body = {
        'providerId': 'provider-a', 'externalTransactionId': '%s-%s' % (ext, RUN),
        'playerId': player, 'walletId': wallet, 'roundId': 'r1', 'gameId': 'g1',
        'kind': kind, 'money': {'amount': amount, 'currency': currency},
    }
    if ref:
        body['referenceExternalTransactionId'] = '%s-%s' % (ref, RUN)
    headers = {} if no_header else {'Idempotency-Key': key or 'provider-a:%s-%s' % (ext, RUN)}
    return http('POST', '/wagering/transactions', body, headers, base)


def rec(wallet_id):
    return http('POST', '/wallets/%s/reconciliation' % wallet_id).json or {}


def tx_by_ext(ext):
    return http('GET', '/providers/provider-a/wagering/transactions/%s-%s' % (ext, RUN))


def desc(r):
    bal = money(r)
    return 'HTTP %s; status=%s; erro/motivo=%s; saldo=%s; replay=%s' % (
        r.status, r.get('status'), r.get('failureCode') or r.get('error'), bal, r.get('idempotentReplay'))


# --------------------------------------------------------------------------- controle da API
def start_api(port=3000, extra_env=None, truncate=False):
    env = dict(os.environ, PORT=str(port))
    env.update(extra_env or {})
    log = open(API_LOG, 'wb' if truncate else 'ab')
    return subprocess.Popen(['bun', 'src/main.ts'], cwd=PROJECT, env=env, stdout=log, stderr=subprocess.STDOUT)


def stop_api(p, timeout=15):
    t0 = time.time()
    if p is None or p.poll() is not None:
        return True, 0.0
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=timeout)
        return True, time.time() - t0
    except subprocess.TimeoutExpired:
        p.kill()
        return False, time.time() - t0


def api_ready(base=None):
    try:
        return http('GET', '/health/ready', base=base).status == 200
    except Exception:
        return False


# --------------------------------------------------------------------------- preparacao (--setup)
def do_setup():
    r = run(['docker', 'compose', 'up', '-d', 'postgres', 'localstack'])
    check('T01', 'Subir Postgres e LocalStack', 'docker compose up -d sem erro', (r.stdout + r.stderr).strip()[-300:], r.returncode == 0)
    pg = wait_until(lambda: sql('SELECT 1')[0] == 0, 90, 2)
    ls = wait_until(lambda: urllib.request.urlopen('http://localhost:4566/_localstack/health', timeout=3).status == 200, 90, 2)
    check('T01b', 'Postgres e LocalStack saudaveis', 'ambos respondendo', 'postgres=%s localstack=%s' % (bool(pg), bool(ls)), pg and ls)
    r = run(['bun', 'run', 'migrate'])
    check('T02', 'Migrations (up)', 'migration up done', (r.stdout + r.stderr).strip()[-200:], r.returncode == 0 and 'up done' in r.stdout)
    tabs = sql_val("SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables WHERE table_schema='public'")
    want = 'inbox_messages,mikro_orm_migrations,outbox_messages,wager_transactions,wallet_ledger_entries,wallets'
    check('T02b', 'Tabelas criadas', want, tabs, tabs == want)
    r = run(['bun', 'run', 'queues'])
    check('T03', 'Criar filas', 'queues ready', (r.stdout + r.stderr).strip()[-200:], r.returncode == 0 and 'queues ready' in r.stdout)
    API_PROC['p'] = start_api(truncate=True)
    ok = wait_until(api_ready, 60, 1)
    check('T04', 'Subir a API', 'API respondendo /health/ready', 'pronta' if ok else 'nao ficou pronta (veja api.log)', ok)
    if not ok:
        raise SystemExit(finish())


# --------------------------------------------------------------------------- testes
def t_health():
    r = http('GET', '/health/live')
    check('T05a', 'Health live', '200 {"status":"ok"}', '%s %s' % (r.status, r.text.strip()), r.status == 200 and r.get('status') == 'ok')
    r = http('GET', '/health/ready')
    check('T05b', 'Health ready (Postgres e SQS)', '200 {"status":"ready"}', '%s %s' % (r.status, r.text.strip()), r.status == 200 and r.get('status') == 'ready')


def t_wallets():
    pa, r = new_wallet('100.00')
    S['A'], S['PA'] = r.get('id'), pa
    check('T06', 'Criar wallet A com 100,00', 'HTTP 201; saldo 100.00; version 1',
          'HTTP %s; saldo %s; version %s' % (r.status, money(r), r.get('version')),
          r.status == 201 and money(r) == '100.00' and r.get('version') == 1)
    out = sql_val("SELECT balance, version FROM wallets WHERE id='%s'" % S['A'])
    check('T06-banco', 'Saldo e versao no banco', '100.00|1', out, out == '100.00|1')
    out = sql_val("SELECT direction, amount, balance_before, balance_after FROM wallet_ledger_entries WHERE wallet_id='%s'" % S['A'])
    check('T06-ledger', 'Lancamento de abertura no ledger', 'CREDIT|100.00|0.00|100.00', out, out == 'CREDIT|100.00|0.00|100.00')
    r = http('POST', '/wallets', {'playerId': pa, 'initialBalance': {'amount': '50.00', 'currency': 'BRL'}})
    check('T07', 'Wallet duplicada (mesmo jogador e moeda)', 'HTTP 409 WALLET_ALREADY_EXISTS', desc(r),
          r.status == 409 and r.get('error') == 'WALLET_ALREADY_EXISTS')
    for a in ['1.001', '-5.00', '1e3', 'abc']:
        _, r = new_wallet(a)
        check('T08[%s]' % a, 'Valor invalido na criacao (%s)' % a, 'HTTP 400 VALIDATION_ERROR', desc(r),
              r.status == 400 and r.get('error') == 'VALIDATION_ERROR')


def t_flow_a():
    A, P = S['A'], S['PA']

    def w(ext, kind, amount, ref=None, **kw):
        return wager(A, P, ext, kind, amount, ref, **kw)

    r1 = w('tx-1', 'BET', '25.00')
    S['tx1'] = r1.get('transactionId')
    check('A1', 'Aposta de 25,00', 'HTTP 200 PROCESSED saldo 75.00 replay=False', desc(r1),
          r1.status == 200 and r1.get('status') == 'PROCESSED' and money(r1) == '75.00' and r1.get('idempotentReplay') is False)

    r = w('tx-1', 'BET', '25.00')
    check('A2', 'Mesmo pedido de novo (replay)', 'HTTP 200; mesmo transactionId; saldo 75.00; replay=True', desc(r),
          r.status == 200 and r.get('transactionId') == S['tx1'] and money(r) == '75.00' and r.get('idempotentReplay') is True)

    r = w('tx-1', 'BET', '26.00')
    check('A3', 'Mesma chave com valor diferente', 'HTTP 409 IDEMPOTENCY_CONFLICT', desc(r),
          r.status == 409 and r.get('error') == 'IDEMPOTENCY_CONFLICT')

    r = w('tx-2', 'BET', '900.00')
    check('A4', 'Aposta maior que o saldo', 'HTTP 422 REJECTED INSUFFICIENT_FUNDS saldo 75.00', desc(r),
          r.status == 422 and r.get('status') == 'REJECTED' and r.get('failureCode') == 'INSUFFICIENT_FUNDS' and money(r) == '75.00')

    r = w('tx-3', 'WIN', '40.00')
    check('A5', 'Ganho de 40,00', 'HTTP 200 PROCESSED saldo 115.00', desc(r),
          r.status == 200 and r.get('status') == 'PROCESSED' and money(r) == '115.00')

    r = w('tx-4', 'LOSS', '10.00')
    n = sql_val("SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id='%s'" % A)
    ver = http('GET', '/wallets/%s' % A).get('version')
    check('A6', 'LOSS nao mexe no saldo nem no ledger', 'HTTP 200 PROCESSED saldo 115.00; 3 lancamentos; version 3',
          '%s; %s lancamentos; version %s' % (desc(r), n, ver),
          r.status == 200 and r.get('status') == 'PROCESSED' and money(r) == '115.00' and n == '3' and ver == 3)

    r = w('tx-5', 'REFUND', '25.00', 'tx-1')
    r_b = w('tx-5', 'REFUND', '25.00', 'tx-1')
    check('A7', 'Devolver a aposta tx-1 (e replay)', '1a: HTTP 200 saldo 140.00; 2a: replay=True saldo 140.00',
          '1a: %s | 2a: %s' % (desc(r), desc(r_b)),
          r.status == 200 and money(r) == '140.00' and r_b.get('idempotentReplay') is True and money(r_b) == '140.00')

    r = w('tx-6', 'ROLLBACK', '25.00', 'tx-1')
    check('A8', 'Reverter de novo a mesma aposta', 'HTTP 422 ALREADY_REVERSED saldo 140.00', desc(r),
          r.status == 422 and r.get('failureCode') == 'ALREADY_REVERSED' and money(r) == '140.00')

    r = w('tx-7', 'ROLLBACK', '40.00', 'tx-3')
    check('A9', 'ROLLBACK do ganho tx-3 (vira debito)', 'HTTP 200 PROCESSED saldo 100.00', desc(r),
          r.status == 200 and r.get('status') == 'PROCESSED' and money(r) == '100.00')

    r = w('tx-8', 'REFUND', '40.00', 'tx-3')
    check('A10', 'REFUND so pode referenciar BET', 'HTTP 422 REFERENCE_KIND_INVALID', desc(r),
          r.status == 422 and r.get('failureCode') == 'REFERENCE_KIND_INVALID')

    r = w('tx-9', 'BET', '10.00')
    r_b = w('tx-10', 'REFUND', '5.00', 'tx-9')
    check('A11', 'Aposta de 10,00 e devolucao com valor errado', 'aposta saldo 90.00; devolucao HTTP 422 AMOUNT_MISMATCH',
          '%s | %s' % (desc(r), desc(r_b)),
          r.status == 200 and money(r) == '90.00' and r_b.status == 422 and r_b.get('failureCode') == 'AMOUNT_MISMATCH')

    r = w('tx-11', 'REFUND', '10.00', 'tx-12')
    check('A12', 'Devolucao antes da aposta (fora de ordem)', 'HTTP 202 PENDING_REFERENCE sem saldo', desc(r),
          r.status == 202 and r.get('status') == 'PENDING_REFERENCE' and money(r) is None)

    r = w('tx-12', 'BET', '10.00')
    check('A13', 'A aposta original chega', 'HTTP 200 PROCESSED saldo 80.00', desc(r),
          r.status == 200 and r.get('status') == 'PROCESSED' and money(r) == '80.00')

    got = wait_until(lambda: (lambda x: x if x.get('status') == 'PROCESSED' else None)(tx_by_ext('tx-11')), 40, 2)
    check('A14', 'Worker retoma a devolucao pendente', 'tx-11 PROCESSED, kind REFUND, saldo 90.00 (ate 40s)',
          'sem resposta PROCESSED em 40s' if not got else 'status=%s kind=%s saldo=%s' % (got.get('status'), got.get('kind'), money(got)),
          bool(got) and got.get('kind') == 'REFUND' and money(got) == '90.00')

    j = rec(A)
    wal = http('GET', '/wallets/%s' % A)
    check('A15', 'Reconciliacao da wallet A', 'consistent=True; 90.00 = 90.00; diferenca 0.00; 8 lancamentos; version 8',
          'consistent=%s; guardado=%s; calculado=%s; dif=%s; lancamentos=%s; version=%s' % (
              j.get('consistent'), (j.get('storedBalance') or {}).get('amount'), (j.get('calculatedBalance') or {}).get('amount'),
              (j.get('difference') or {}).get('amount'), j.get('checkedEntries'), wal.get('version')),
          j.get('consistent') is True and (j.get('storedBalance') or {}).get('amount') == '90.00'
          and (j.get('difference') or {}).get('amount') == '0.00' and j.get('checkedEntries') == 8
          and wal.get('version') == 8 and money(wal) == '90.00')

    wait_until(lambda: sql_val("SELECT count(*) FROM outbox_messages WHERE published_at IS NULL AND aggregate_id='%s'" % A) == '0', 30, 2)
    rows = sql_val("SELECT event_type, count(*) FROM outbox_messages WHERE aggregate_id='%s' GROUP BY 1 ORDER BY 1" % A)
    counts = {}
    for line in rows.splitlines():
        if '|' in line:
            k, v = line.split('|')
            counts[k] = int(v)
    want = {'WagerTransactionPendingReference': 1, 'WagerTransactionProcessed': 9,
            'WagerTransactionRejected': 4, 'WalletBalanceChanged': 8}
    pend = sql_val("SELECT count(*) FROM outbox_messages WHERE published_at IS NULL AND aggregate_id='%s'" % A)
    check('A16', 'Eventos do outbox da wallet A (22 no total, 0 pendentes)', '%s; pendentes 0' % want,
          '%s; pendentes %s' % (counts, pend), counts == want and pend == '0')


def t_flow_b():
    pb, r = new_wallet('10.00')
    B = r.get('id')
    S['B'], S['PB'] = B, pb
    r1 = wager(B, pb, 'b1', 'WIN', '50.00')
    r2 = wager(B, pb, 'b2', 'BET', '55.00')
    r3 = wager(B, pb, 'b3', 'ROLLBACK', '50.00', 'b1')
    j = rec(B)
    check('B1-B4', 'Reversao que deixaria saldo negativo',
          'WIN 60.00; BET 5.00; ROLLBACK 422 REVERSAL_WOULD_OVERDRAW saldo 5.00; consistente, 3 lancamentos',
          '%s | %s | %s | consistent=%s guardado=%s lancamentos=%s' % (
              money(r1), money(r2), desc(r3), j.get('consistent'), (j.get('storedBalance') or {}).get('amount'), j.get('checkedEntries')),
          money(r1) == '60.00' and money(r2) == '5.00' and r3.status == 422 and r3.get('failureCode') == 'REVERSAL_WOULD_OVERDRAW'
          and money(r3) == '5.00' and j.get('consistent') is True and j.get('checkedEntries') == 3)

    for tid, title, args_ in [
        ('B5a', 'Valor com 3 casas (1.001)', ('bad1', 'BET', '1.001')),
        ('B5b', 'Valor negativo (-5.00)', ('bad2', 'BET', '-5.00')),
        ('B5c', 'Tipo OPENING (interno) recusado', ('bad3', 'OPENING', '10.00')),
        ('B5d', 'REFUND sem referencia', ('bad4', 'REFUND', '10.00')),
    ]:
        r = wager(B, pb, *args_)
        check(tid, title, 'HTTP 400 VALIDATION_ERROR', desc(r), r.status == 400 and r.get('error') == 'VALIDATION_ERROR')
    r = wager(B, pb, 'bad-h', 'BET', '1.00', no_header=True)
    check('B5e', 'Sem o header Idempotency-Key', 'HTTP 400 VALIDATION_ERROR', desc(r), r.status == 400 and r.get('error') == 'VALIDATION_ERROR')

    r = wager(B, pb, 'bad5', 'BET', '10.00', currency='USD')
    check('B6a', 'Moeda diferente da wallet', 'HTTP 422 CURRENCY_MISMATCH', desc(r), r.status == 422 and r.get('failureCode') == 'CURRENCY_MISMATCH')
    r = wager(B, uuid7(), 'bad6', 'BET', '10.00')
    check('B6b', 'Jogador que nao e dono da wallet', 'HTTP 422 PLAYER_MISMATCH', desc(r), r.status == 422 and r.get('failureCode') == 'PLAYER_MISMATCH')
    r = wager(uuid7(), pb, 'bad7', 'BET', '10.00')
    check('B6c', 'Wallet inexistente', 'HTTP 404 WALLET_NOT_FOUND', desc(r), r.status == 404 and r.get('error') == 'WALLET_NOT_FOUND')
    j = rec(B)
    check('B6d', 'Wallet B intacta depois dos erros', 'saldo 5.00; 3 lancamentos; consistent=True',
          'saldo %s; %s lancamentos; consistent=%s' % ((j.get('storedBalance') or {}).get('amount'), j.get('checkedEntries'), j.get('consistent')),
          (j.get('storedBalance') or {}).get('amount') == '5.00' and j.get('checkedEntries') == 3 and j.get('consistent') is True)


def t_queries():
    A = S['A']
    r = tx_by_ext('tx-1')
    check('C1a', 'Consultar transacao pelo id do provedor', 'HTTP 200 PROCESSED BET saldo 75.00',
          'HTTP %s status=%s kind=%s saldo=%s' % (r.status, r.get('status'), r.get('kind'), money(r)),
          r.status == 200 and r.get('status') == 'PROCESSED' and r.get('kind') == 'BET' and money(r) == '75.00')
    r = http('GET', '/providers/provider-a/wagering/transactions/nao-existe')
    check('C1b', 'Transacao inexistente', 'HTTP 404 NOT_FOUND', desc(r), r.status == 404 and r.get('error') == 'NOT_FOUND')
    r = http('GET', '/wallets/abc')
    check('C1c', 'Id de wallet que nao e UUID', 'HTTP 400', desc(r), r.status == 400)
    r = http('GET', '/wagering/transactions/%s' % S['tx1'])
    check('C1d', 'Consultar transacao pelo id interno', 'HTTP 200 PROCESSED', 'HTTP %s status=%s' % (r.status, r.get('status')),
          r.status == 200 and r.get('status') == 'PROCESSED')

    p1 = http('GET', '/wallets/%s/ledger?limit=3' % A)
    c1 = p1.get('nextCursor')
    p2 = http('GET', '/wallets/%s/ledger?limit=3&cursor=%s' % (A, urllib.parse.quote(c1 or '')))
    c2 = p2.get('nextCursor')
    p3 = http('GET', '/wallets/%s/ledger?limit=3&cursor=%s' % (A, urllib.parse.quote(c2 or '')))
    n1, n2, n3 = [len(p.get('items') or []) for p in (p1, p2, p3)]
    first = (p1.get('items') or [{}])[0].get('direction')
    check('C2', 'Extrato paginado (8 lancamentos, 3 por pagina)',
          'paginas de 3, 3 e 2 itens; 1o item CREDIT; ultimo cursor null',
          'itens %s/%s/%s; 1o=%s; cursor1=%s; cursor3=%s' % (n1, n2, n3, first, bool(c1), p3.get('nextCursor')),
          (n1, n2, n3) == (3, 3, 2) and first == 'CREDIT' and c1 and c2 and p3.get('nextCursor') is None)
    r = http('GET', '/wallets/%s/ledger?cursor=abc' % A)
    check('C2b', 'Cursor invalido', 'HTTP 400', desc(r), r.status == 400)


def t_db():
    A = S['A']
    rc, out = sql("UPDATE wallet_ledger_entries SET amount = 1 WHERE wallet_id='%s'" % A)
    check('D1a', 'Ledger imutavel (UPDATE)', 'ERROR ... append-only', out[:160], 'append-only' in out)
    rc, out = sql("DELETE FROM wallet_ledger_entries WHERE wallet_id='%s'" % A)
    check('D1b', 'Ledger imutavel (DELETE)', 'ERROR ... append-only', out[:160], 'append-only' in out)
    rc, out = sql("UPDATE wallets SET balance = -1 WHERE id='%s'" % A)
    check('D2', 'Saldo negativo impossivel (CHECK)', 'ERROR ... violates check constraint', out[:160], 'violates check constraint' in out)

    before = metric('wager_reconciliation_divergences_total')
    sql("UPDATE wallets SET balance = balance + 1.00 WHERE id='%s'" % A)
    j = rec(A)
    after = metric('wager_reconciliation_divergences_total')
    sql("UPDATE wallets SET balance = balance - 1.00 WHERE id='%s'" % A)
    j2 = rec(A)
    check('D3', 'Divergencia sinalizada e nunca corrigida sozinha',
          'consistent=False 91.00 x 90.00 dif 1.00; metrica sobe; depois de desfazer consistent=True',
          'consistent=%s %s x %s dif %s; metrica %s -> %s; depois consistent=%s' % (
              j.get('consistent'), (j.get('storedBalance') or {}).get('amount'), (j.get('calculatedBalance') or {}).get('amount'),
              (j.get('difference') or {}).get('amount'), before, after, j2.get('consistent')),
          j.get('consistent') is False and (j.get('storedBalance') or {}).get('amount') == '91.00'
          and (j.get('difference') or {}).get('amount') == '1.00' and after > before and j2.get('consistent') is True)

    if os.path.exists(API_LOG) and API_PROC['p'] is not None:
        lines = [l for l in open(API_LOG, errors='replace') if 'wager handled' in l]
        leaked = [l for l in lines if '"amount"' in l or '"money"' in l]
        check('D4', 'Logs estruturados sem valores monetarios', 'linhas "wager handled" sem amount/money',
              '%d linhas; %d com valor monetario' % (len(lines), len(leaked)), len(lines) > 0 and not leaked)
    else:
        skip('D4', 'Logs sem dados sensiveis', 'rode com --setup para o script ler o api.log; ou confira o terminal da API')


TS_SEND = """import { GetQueueUrlCommand, SendMessageCommand, SQSClient } from '@aws-sdk/client-sqs';
import { config } from '../src/config';

const sqs = new SQSClient({
  region: config.sqs.region, endpoint: config.sqs.endpoint,
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const { QueueUrl } = await sqs.send(new GetQueueUrlCommand({ QueueName: 'wager-transactions.fifo' }));

// bun scripts/send-message.ts <walletId> <playerId> <externalId> <kind> <amount> <messageId>
// bun scripts/send-message.ts raw     (mensagem que nao e JSON)
const [walletId, playerId, ext, kind, amount, messageId] = process.argv.slice(2);
const raw = walletId === 'raw';
const body = raw ? 'isto nao e json' : JSON.stringify({
  messageId, type: 'WagerTransactionRequested', occurredAt: new Date().toISOString(),
  data: {
    providerId: 'provider-a', externalTransactionId: ext, idempotencyKey: 'provider-a:' + ext,
    playerId, walletId, roundId: 'r1', gameId: 'g1', kind, money: { amount, currency: 'BRL' },
  },
});
await sqs.send(new SendMessageCommand({
  QueueUrl, MessageBody: body, MessageGroupId: raw ? 'raw' : walletId,
  MessageDeduplicationId: crypto.randomUUID(),
}));
console.log('sent');
"""


def sendmsg(ext, kind, amount, msgid, wallet=None, player=None):
    cmd = ['bun', 'scripts/send-message.ts', wallet or S['C'], player or S['PC'], ext, kind, amount, msgid]
    p = run(cmd)
    return p.returncode == 0 and 'sent' in p.stdout


def t_sqs():
    if not shutil.which('bun'):
        skip('S*', 'Testes de fila SQS', 'bun nao encontrado no PATH')
        return
    path = os.path.join(PROJECT, 'scripts', 'send-message.ts')
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        open(path, 'w').write(TS_SEND)
    pc, r = new_wallet('100.00')
    S['C'], S['PC'] = r.get('id'), pc
    C = S['C']

    def bal():
        return money(http('GET', '/wallets/%s' % C))

    ok = sendmsg('sqs-1-%s' % RUN, 'BET', '10.00', 'm1-%s' % RUN)
    wait_until(lambda: bal() == '90.00', 20, 1)
    done = sql_val("SELECT processed_at IS NOT NULL FROM inbox_messages WHERE message_id='m1-%s'" % RUN)
    check('S1', 'Mensagem normal pela fila', 'saldo 90.00; inbox processada (t)', 'envio=%s; saldo %s; inbox=%s' % (ok, bal(), done),
          ok and bal() == '90.00' and done == 't')

    dup0 = metric('wager_duplicates_total', 'source="sqs"')
    sendmsg('sqs-1-%s' % RUN, 'BET', '10.00', 'm1-%s' % RUN)
    time.sleep(5)
    n_inbox = sql_val("SELECT count(*) FROM inbox_messages WHERE message_id='m1-%s'" % RUN)
    dup1 = metric('wager_duplicates_total', 'source="sqs"')
    check('S2', 'Mesma mensagem (mesmo messageId) de novo', 'saldo 90.00; 1 linha na inbox; duplicatas sobem',
          'saldo %s; inbox %s; duplicatas %s -> %s' % (bal(), n_inbox, dup0, dup1),
          bal() == '90.00' and n_inbox == '1' and dup1 >= dup0 + 1)

    sendmsg('sqs-1-%s' % RUN, 'BET', '10.00', 'm2-%s' % RUN)
    time.sleep(5)
    n_tx = sql_val("SELECT count(*) FROM wager_transactions WHERE external_transaction_id='sqs-1-%s'" % RUN)
    check('S3', 'Mesma operacao com outro messageId', 'saldo 90.00; 1 transacao com esse id externo',
          'saldo %s; %s transacao(oes)' % (bal(), n_tx), bal() == '90.00' and n_tx == '1')

    sendmsg('sqs-2-%s' % RUN, 'BET', '500.00', 'm3-%s' % RUN)
    got = wait_until(lambda: (lambda x: x if x.status == 200 else None)(tx_by_ext('sqs-2')), 20, 1)
    check('S4', 'Rejeicao de negocio pela fila (ack, nao DLQ)', 'REJECTED INSUFFICIENT_FUNDS; saldo 90.00',
          'sem resposta' if not got else 'status=%s motivo=%s saldo=%s' % (got.get('status'), got.get('failureCode'), bal()),
          bool(got) and got.get('status') == 'REJECTED' and got.get('failureCode') == 'INSUFFICIENT_FUNDS' and bal() == '90.00')

    dlq0 = metric('wager_dlq_messages_total')
    sendmsg('sqs-1-%s' % RUN, 'BET', '11.00', 'm4-%s' % RUN)                       # conflito de idempotencia
    run(['bun', 'scripts/send-message.ts', 'raw'])                                  # lixo
    sendmsg('x-%s' % RUN, 'BET', '1.00', 'm5-%s' % RUN, wallet=uuid7())             # wallet inexistente
    wait_until(lambda: metric('wager_dlq_messages_total') >= dlq0 + 3, 30, 1)
    dlq1 = metric('wager_dlq_messages_total')
    check('S5-S7', 'Erros permanentes vao para a DLQ', 'DLQ +3 (conflito, lixo, wallet inexistente); saldo 90.00',
          'DLQ %s -> %s; saldo %s' % (dlq0, dlq1, bal()), dlq1 >= dlq0 + 3 and bal() == '90.00')


def t_concurrency():
    pd, r = new_wallet('100.00')
    D = r.get('id')
    S['D'] = D
    with ThreadPoolExecutor(50) as ex:
        res = list(ex.map(lambda _: wager(D, pd, 'conc-1', 'BET', '10.00'), range(50)))
    ok200 = sum(1 for x in res if x.status == 200)
    firsts = sum(1 for x in res if x.get('idempotentReplay') is False)
    j = rec(D)
    check('K1', 'Mesma aposta 50x em paralelo', '50 respostas 200; 1 processamento real; saldo 90.00; 2 lancamentos',
          '%d x 200; %d processamento(s) real(is); saldo %s; %s lancamentos; consistent=%s' % (
              ok200, firsts, (j.get('storedBalance') or {}).get('amount'), j.get('checkedEntries'), j.get('consistent')),
          ok200 == 50 and firsts == 1 and (j.get('storedBalance') or {}).get('amount') == '90.00'
          and j.get('checkedEntries') == 2 and j.get('consistent') is True)

    pe, r = new_wallet('100.00')
    E = r.get('id')
    with ThreadPoolExecutor(2) as ex:
        f1 = ex.submit(wager, E, pe, 'd1', 'BET', '80.00')
        f2 = ex.submit(wager, E, pe, 'd2', 'BET', '80.00')
        st = sorted([f1.result().status, f2.result().status])
    j = rec(E)
    check('K2', 'Cenario obrigatorio: duas apostas de 80 disputando 100', 'status [200, 422]; saldo 20.00; 2 lancamentos',
          'status %s; saldo %s; %s lancamentos; consistent=%s' % (
              st, (j.get('storedBalance') or {}).get('amount'), j.get('checkedEntries'), j.get('consistent')),
          st == [200, 422] and (j.get('storedBalance') or {}).get('amount') == '20.00' and j.get('checkedEntries') == 2 and j.get('consistent') is True)

    pf, r = new_wallet('100.00')
    Fw = r.get('id')
    with ThreadPoolExecutor(60) as ex:
        res = list(ex.map(lambda i: wager(Fw, pf, 'conc-3', 'BET', '1.00', base=BASES[i % len(BASES)]), range(60)))
    ok200 = sum(1 for x in res if x.status == 200)
    j = rec(Fw)
    check('K3', 'Mesma operacao 60x em %d instancia(s)' % len(BASES), '60 respostas 200; saldo 99.00; consistent=True',
          '%d x 200; saldo %s; consistent=%s' % (ok200, (j.get('storedBalance') or {}).get('amount'), j.get('consistent')),
          ok200 == 60 and (j.get('storedBalance') or {}).get('amount') == '99.00' and j.get('consistent') is True)
    if len(BASES) < 3:
        record('K3-nota', 'Aviso', 'ao menos 3 instancias', 'rodou com %d instancia(s); use --bases para 3' % len(BASES), False, status='SKIP')

    ok = wait_until(lambda: sql_val('SELECT count(*) FROM outbox_messages WHERE published_at IS NULL') == '0', 40, 2)
    check('K4', 'Outbox totalmente publicado', 'pendentes 0', 'pendentes %s' % sql_val('SELECT count(*) FROM outbox_messages WHERE published_at IS NULL'), bool(ok))


def t_failures():
    pf, r = new_wallet('100.00')
    Fw = r.get('id')
    S['F'] = Fw
    run(['docker', 'compose', 'stop', 'postgres'])
    live = http('GET', '/health/live')
    ready = http('GET', '/health/ready')
    r1 = wager(Fw, pf, 'f1', 'BET', '1.00')
    check('F2a', 'Postgres fora: live 200, ready 503, aposta 503 com Retry-After',
          'live 200; ready 503; aposta 503 TRANSIENT_ERROR + Retry-After',
          'live %s; ready %s; aposta %s %s retry-after=%s' % (live.status, ready.status, r1.status, r1.get('error'), r1.headers.get('Retry-After')),
          live.status == 200 and ready.status == 503 and r1.status == 503 and r1.get('error') == 'TRANSIENT_ERROR' and bool(r1.headers.get('Retry-After')))
    run(['docker', 'compose', 'start', 'postgres'])
    wait_until(lambda: sql('SELECT 1')[0] == 0, 90, 2)
    final = wait_until(lambda: (lambda x: x if x.status == 200 else None)(wager(Fw, pf, 'f1', 'BET', '1.00')), 90, 3)
    n = sql_val("SELECT count(*) FROM wager_transactions WHERE external_transaction_id='f1-%s'" % RUN)
    check('F2b', 'Reenvio com a mesma chave depois que o Postgres volta', 'HTTP 200 e exatamente 1 transacao',
          'sem 200 em 90s' if not final else 'HTTP 200; %s transacao(oes); saldo %s' % (n, money(final)), bool(final) and n == '1')

    run(['docker', 'compose', 'stop', 'localstack'])
    ready = http('GET', '/health/ready')
    r2 = wager(Fw, pf, 'f2', 'BET', '1.00')
    check('F3a', 'SQS fora: ready 503, mas a aposta HTTP continua 200', 'ready 503; aposta 200',
          'ready %s; aposta %s' % (ready.status, r2.status), ready.status == 503 and r2.status == 200)
    run(['docker', 'compose', 'start', 'localstack'])
    wait_until(lambda: urllib.request.urlopen('http://localhost:4566/_localstack/health', timeout=3).status == 200, 90, 2)
    run(['bun', 'run', 'queues'])
    ok = wait_until(lambda: sql_val('SELECT count(*) FROM outbox_messages WHERE published_at IS NULL') == '0', 90, 2)
    check('F3b', 'Depois de religar o SQS (e recriar as filas), o outbox drena', 'pendentes 0',
          'pendentes %s' % sql_val('SELECT count(*) FROM outbox_messages WHERE published_at IS NULL'), bool(ok))


def t_crash():
    if API_PROC['p'] is None:
        skip('S8', 'Crash depois do commit e antes do ack', 'exige --setup (o script precisa controlar a API)')
        return
    C = S.get('C')
    if not C:
        skip('S8', 'Crash depois do commit e antes do ack', 'wallet C nao foi criada (testes de SQS nao rodaram)')
        return
    stop_api(API_PROC['p'])
    crash = start_api(3100, {'CRASH_BEFORE_ACK': '1'})
    wait_until(lambda: http('GET', '/health/live', base='http://localhost:3100').status == 200, 40, 1)
    sendmsg('sqs-9-%s' % RUN, 'BET', '10.00', 'm9-%s' % RUN)
    try:
        crash.wait(timeout=40)
        died = True
    except subprocess.TimeoutExpired:
        crash.kill()
        died = False
    bal_db = sql_val("SELECT balance FROM wallets WHERE id='%s'" % C)
    check('S8a', 'Instancia morre depois do commit e antes do ack', 'processo morreu; debito ja gravado (saldo 80.00)',
          'morreu=%s; saldo no banco %s' % (died, bal_db), died and bal_db == '80.00')
    API_PROC['p'] = start_api()
    wait_until(api_ready, 60, 1)
    got = wait_until(lambda: metric('wager_duplicates_total', 'source="sqs"') >= 1, 90, 3)
    n = sql_val("SELECT count(*) FROM wager_transactions WHERE external_transaction_id='sqs-9-%s'" % RUN)
    bal_db = sql_val("SELECT balance FROM wallets WHERE id='%s'" % C)
    check('S8b', 'Redelivery nao duplica o debito', 'mensagem reentregue (ate 90s); saldo 80.00; 1 transacao',
          'reentregue=%s; saldo %s; %s transacao(oes)' % (bool(got), bal_db, n), bool(got) and bal_db == '80.00' and n == '1')


def t_unit():
    if not shutil.which('bun'):
        skip('U1', 'Typecheck e testes de unidade', 'bun nao encontrado')
        return
    p = run(['bun', 'run', 'typecheck'])
    check('U1', 'bun run typecheck', 'sem erros de tipo', (p.stdout + p.stderr).strip()[-200:] or 'ok', p.returncode == 0)
    p = run(['bun', 'test', 'src'])
    out = p.stdout + p.stderr
    m = re.search(r'(\d+) pass', out)
    f = re.search(r'(\d+) fail', out)
    check('U2', 'bun test src (unidade)', '7 testes passando, 0 falhas',
          '%s passando, %s falhas' % (m.group(1) if m else '?', f.group(1) if f else '0'),
          p.returncode == 0 and m and m.group(1) == '7' and not (f and f.group(1) != '0'))


def t_shutdown_and_restart():
    p = API_PROC['p']
    if p is None:
        skip('F1', 'Desligamento gracioso (SIGTERM)', 'exige --setup')
        skip('F4', 'Reinicio com consistencia', 'exige --setup')
        return
    ok, secs = stop_api(p)
    check('F1', 'Desligamento gracioso com SIGTERM', 'processo termina em ate 15s sem ser morto', 'terminou=%s em %.1fs' % (ok, secs), ok)
    API_PROC['p'] = start_api()
    ready = wait_until(api_ready, 60, 1)
    bad = []
    for k in ('A', 'B', 'C', 'D', 'F'):
        if S.get(k):
            if rec(S[k]).get('consistent') is not True:
                bad.append(k)
    check('F4', 'Reinicio: todas as wallets seguem consistentes', 'consistent=True em todas', 'API pronta=%s; inconsistentes: %s' % (bool(ready), bad or 'nenhuma'),
          bool(ready) and not bad)


# --------------------------------------------------------------------------- relatorio
def finish():
    total = len(RESULTS)
    cnt = {k: sum(1 for r in RESULTS if r['status'] == k) for k in ('PASS', 'FAIL', 'SKIP')}
    lines = []
    lines.append('RESULTADOS DOS TESTES - Distributed Wagering Processor')
    lines.append('=' * 70)
    lines.append('Data    : %s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    lines.append('API     : %s' % ', '.join(BASES))
    lines.append('Execucao: RUN=%s (sufixo dos ids externos)' % RUN)
    lines.append('Pasta   : %s' % PROJECT)
    lines.append('')
    for r in RESULTS:
        lines.append('[%s] %s - %s' % (r['status'], r['id'], r['title']))
        if r['expected']:
            lines.append('    Esperado: %s' % r['expected'])
        lines.append('    Obtido  : %s' % r['actual'])
        lines.append('')
    lines.append('=' * 70)
    lines.append('RESUMO: %d verificacoes | PASS %d | FAIL %d | SKIP %d' % (total, cnt['PASS'], cnt['FAIL'], cnt['SKIP']))
    fails = [r for r in RESULTS if r['status'] == 'FAIL']
    if fails:
        lines.append('')
        lines.append('FALHAS:')
        for r in fails:
            lines.append('  - %s: %s' % (r['id'], r['title']))
    with open(OUT, 'w', encoding='utf-8') as fh:
        fh.write('\n'.join(lines) + '\n')
    print('\nResumo: PASS %d | FAIL %d | SKIP %d' % (cnt['PASS'], cnt['FAIL'], cnt['SKIP']))
    print('Relatorio gravado em: %s' % OUT)
    return 1 if cnt['FAIL'] else 0


def main():
    if not os.path.exists(os.path.join(PROJECT, 'docker-compose.yml')):
        print('Nao achei docker-compose.yml em %s. Rode dentro da pasta do projeto ou use --project.' % PROJECT)
        return 2
    try:
        if args.setup:
            do_setup()
        elif not api_ready():
            print('A API nao respondeu em %s. Suba com "bun start" ou rode com --setup.' % BASE)
            return 2
    except urllib.error.URLError:
        print('Nao consegui falar com %s. Suba a API ("bun start") ou use --setup.' % BASE)
        return 2

    for fn in (t_health, t_wallets, t_flow_a, t_flow_b, t_queries, t_db, t_sqs, t_concurrency):
        safe(fn)
    if args.crash:
        safe(t_crash)
    if args.failures:
        safe(t_failures)
    safe(t_unit)
    safe(t_shutdown_and_restart)

    p = API_PROC['p']
    if p is not None and p.poll() is None:
        stop_api(p)
    return finish()


if __name__ == '__main__':
    sys.exit(main())

