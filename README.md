# Distributed Wagering Processor

Serviço financeiro distribuído que processa transações de apostas (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) recebidas de provedores de jogos, por **API HTTP** e por **fila AWS SQS**. O foco é a correção financeira: o saldo nunca fica negativo, nenhuma operação é cobrada ou creditada duas vezes, e o saldo da wallet é sempre igual à soma do extrato (ledger), mesmo com mensagens duplicadas, fora de ordem, simultâneas ou com falhas no meio do caminho.

Não há interface gráfica: o projeto é uma API, um consumidor de fila e workers em segundo plano.

## Índice

1. [O que foi feito](#1-o-que-foi-feito)
2. [Estrutura do projeto](#2-estrutura-do-projeto)
3. [Pré-requisitos](#3-pré-requisitos)
4. [Tutorial 1: preparar o ambiente (`setup_ambiente.py`)](#4-tutorial-1-preparar-o-ambiente-setup_ambientepy)
5. [Subir a API](#5-subir-a-api)
6. [Tutorial 2: rodar os testes (`script.py`)](#6-tutorial-2-rodar-os-testes-scriptpy)
7. [Rodar 3 instâncias no Docker](#7-rodar-3-instâncias-no-docker)
8. [Referência rápida](#8-referência-rápida)
9. [Problemas comuns](#9-problemas-comuns)
10. [Limitações conhecidas](#10-limitações-conhecidas)

---

## 1. O que foi feito

### Tecnologias

| Tecnologia | Uso |
|---|---|
| Bun 1.x | runtime, gerenciador de pacotes e executor de testes |
| TypeScript (strict) | linguagem |
| NestJS | framework (controllers, módulos, injeção de dependência) |
| PostgreSQL 16 | banco de dados, fonte da verdade das invariantes |
| MikroORM 6 | gerencia a transação e as migrations; SQL explícito nos pontos críticos |
| decimal.js | aritmética decimal exata para dinheiro (nunca `number`) |
| zod | validação de entrada (HTTP e SQS) |
| AWS SQS via LocalStack | filas FIFO e DLQ, simuladas localmente |
| pino | logs estruturados em JSON |
| prom-client | métricas no formato Prometheus |
| Docker Compose | Postgres, LocalStack e a aplicação |

### Arquitetura em camadas

- `domain/`: regras de negócio puras (`Money`, `Wallet`, `WagerTransaction`, `WalletLedgerEntry`, eventos, inbox e outbox). Não importa NestJS, ORM nem banco.
- `application/`: casos de uso. Um único `ProcessWagerService` atende a API **e** a fila, para a regra financeira existir em um lugar só.
- `infrastructure/`: Postgres, SQS, consumidor, workers, logs e métricas.
- `http/`: controllers, filtro de erros e ponto de extensão de autenticação.

### Regras de negócio

| Operação | Efeito no saldo | Ledger |
|---|---|---|
| `BET` | débito (rejeita se não houver saldo) | 1 lançamento `DEBIT` |
| `WIN` | crédito | 1 lançamento `CREDIT` |
| `LOSS` | nenhum | nenhum |
| `REFUND` | crédito, reverte uma `BET` processada | 1 lançamento `CREDIT` |
| `ROLLBACK` | inverso da referência (`BET`, `WIN` ou `REFUND`) | 1 lançamento invertido |

- `REFUND` e `ROLLBACK` exigem a referência (`referenceExternalTransactionId`), que deve ser do mesmo provedor, jogador, wallet, moeda e rodada, com o **mesmo valor**.
- Uma transação processada só pode ser revertida **uma vez** (por qualquer reversor).
- Reversão que deixaria o saldo negativo é rejeitada com `REVERSAL_WOULD_OVERDRAW`, código diferente de `INSUFFICIENT_FUNDS`.
- Rejeição de negócio **não altera saldo e não gera ledger**, mas fica gravada (`REJECTED`) para auditoria.
- Referência ausente: a transação fica `PENDING_REFERENCE` e um worker a reprocessa com backoff exponencial (5 s, 10 s, 20 s, 40 s, 80 s, 160 s, 300 s; 8 avaliações no total, cerca de 10 minutos). Esgotado o prazo, vira `REJECTED` com `REFERENCE_NOT_FOUND`.

### Como as garantias são obtidas

- **Concorrência:** lock pessimista por wallet (`SELECT ... FOR UPDATE`). Duas operações na mesma wallet são serializadas; wallets diferentes rodam em paralelo; funciona com várias instâncias porque o lock está no banco. O `UPDATE` do saldo ainda confere a `version` como segunda defesa.
- **Idempotência persistente:** chaves `UNIQUE` no banco (`idempotency_key` e `(provider_id, external_transaction_id)`) e `payload_hash` (SHA-256 do JSON canônico com chaves ordenadas). Mesma chave e mesmo payload devolve a resposta original (`idempotentReplay: true`); mesma chave com payload diferente é conflito (409).
- **Atomicidade:** transação, saldo, ledger, inbox e outbox são gravados na **mesma transação SQL**.
- **Inbox:** a chave `(consumer_name, message_id)` impede processar a mesma mensagem da fila duas vezes. O `ack` só acontece depois do commit.
- **Outbox transacional:** o evento é gravado na mesma transação do dinheiro e publicado depois por um worker com `FOR UPDATE SKIP LOCKED`, que funciona com vários publishers. Se o processo morrer depois do commit, outra instância publica. Publicação duplicada é possível (entrega at-least-once) e segura, porque o `eventId` é estável.
- **Schema como defesa:** `CHECK (balance >= 0)`, `UNIQUE (player_id, currency)`, índice único parcial que impede reversão dupla, `CHECK` aritmético e `UNIQUE (wallet_id, transaction_id)` no ledger, e um **trigger** que bloqueia `UPDATE` e `DELETE` no ledger.

### Tabelas

| Tabela | Para quê |
|---|---|
| `wallets` | saldo atual por jogador e moeda (`version` sobe a cada mudança de saldo) |
| `wager_transactions` | cada operação recebida, com status, motivo de rejeição, hash do payload e saldo no momento do resultado |
| `wallet_ledger_entries` | extrato imutável: direção, valor, saldo antes e depois |
| `inbox_messages` | mensagens da fila já recebidas (deduplicação) |
| `outbox_messages` | eventos a publicar (`published_at` vazio significa pendente) |

### Filas

| Fila | Uso |
|---|---|
| `wager-transactions.fifo` | pedidos de apostas (entrada) |
| `wager-transactions-dlq.fifo` | mensagens sem conserto ou que estouraram 5 tentativas |
| `wager-events.fifo` | eventos de integração publicados pelo outbox |

O consumidor distingue: **erro de negócio** (grava `REJECTED` e dá ack), **transitório** (sem ack, volta com backoff) e **permanente** (JSON inválido, wallet inexistente, conflito de idempotência: vai para a DLQ).

### Eventos de integração

`WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` e `WalletBalanceChanged` (somente quando o saldo muda). Todos com `eventId`, `aggregateId` (a wallet), `correlationId`, `causationId`, `occurredAt`, `version` e `data`, com dinheiro em string decimal.

### Observabilidade

- Logs JSON com `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId`, sem valores monetários nem payloads.
- Métricas em `/metrics`: transações por status, duplicatas, retries, mensagens na DLQ, conflitos de lock, divergências de reconciliação, lag do outbox e latência.
- `/health/live` (processo vivo) e `/health/ready` (Postgres e SQS alcançáveis), ambos sem autenticação.

### Autenticação

Não implementada, por decisão: não vale pontos no desafio. Existe um `AuthGuard` sem efeito como ponto de extensão. O desenho futuro é validar um JWT emitido por um Identity Provider externo (Keycloak ou Zitadel) e checar o escopo do provedor.

---

## 2. Estrutura do projeto

```
jungle-wagering/
├── setup_ambiente.py          # Tutorial 1: prepara tudo (Windows e Linux)
├── script.py                  # Tutorial 2: roda o roteiro de testes e grava o .txt
├── README.md
├── GUIA_DE_TESTES.md          # roteiro manual com curl, comando a comando
├── docker-compose.yml
├── Dockerfile
├── package.json
├── tsconfig.json
├── scripts/
│   ├── create-queues.ts       # cria as filas SQS
│   └── send-message.ts        # envia mensagens de teste à fila (criado pelo script.py se faltar)
├── src/
│   ├── main.ts
│   ├── app.module.ts
│   ├── config.ts
│   ├── migrate.ts
│   ├── shared-ids.ts
│   ├── domain/                # money, wallet, wager-transaction, ledger-entry, events, messaging, errors, failure-code
│   ├── application/           # process-wager, create-wallet, query, payload-hash, schemas, errors
│   ├── infrastructure/        # db, store, sqs, wager.consumer, pending.worker, outbox.publisher, logger, metrics, migrations/
│   └── http/                  # wagering.controller, ops.controller, exception.filter, auth.guard
└── test/                      # helpers, concurrency.test.ts, sqs.test.ts
```

---

## 3. Pré-requisitos

| Item | Linux | Windows |
|---|---|---|
| Docker com Compose v2 (`docker compose`) | Docker Engine | Docker Desktop (com WSL 2) |
| Bun 1.x | o `setup_ambiente.py` instala se faltar | idem |
| Python 3.8+ | `python3` | `python` ou `py` (https://python.org) |
| Terminal | qualquer | PowerShell ou CMD |

Não é preciso instalar Postgres nem SQS: eles rodam em containers.

Nos comandos abaixo, use `python3` no Linux/Mac e `python` (ou `py`) no Windows.

> No Windows, mantenha o **Docker Desktop aberto e em execução** antes de rodar qualquer script.

---

## 4. Tutorial 1: preparar o ambiente (`setup_ambiente.py`)

Este script baixa e prepara tudo o que o projeto precisa. Rode na **raiz do projeto** (a pasta onde estão `docker-compose.yml` e `package.json`).

**Linux/Mac:**

```bash
cd caminho/para/jungle-wagering
python3 setup_ambiente.py
```

**Windows (PowerShell):**

```powershell
cd C:\caminho\para\jungle-wagering
python setup_ambiente.py
```

### O que ele faz, em 7 passos

1. Confere os pré-requisitos: Python, Docker instalado e **em execução**.
2. Confere o Bun. Se não existir, pergunta se pode instalar (ou instala direto com `--install-bun`).
3. Baixa as dependências do projeto (`bun install`).
4. Baixa as imagens Docker `postgres` e `localstack`.
5. Sobe o Postgres e o LocalStack e espera ficarem prontos.
6. Roda as migrations (cria as tabelas).
7. Cria as filas SQS.

### O que você deve ver no final

```
============================================================
AMBIENTE PRONTO
============================================================
Proximos passos:
  1) Subir a API:        bun start        (ou rode este script com --start)
  2) Rodar os testes:    python3 script.py
```

Se algum passo falhar, o script para e mostra a causa e o que fazer (veja a seção [Problemas comuns](#9-problemas-comuns)). Pode rodá-lo de novo quantas vezes quiser: ele é seguro de repetir.

### Opções

| Opção | O que faz |
|---|---|
| `--start` | depois de preparar tudo, sobe a API em primeiro plano (`bun start`) |
| `--install-bun` | instala o Bun sem perguntar, se não existir |
| `--status` | mostra o estado dos containers e da API |
| `--down` | derruba os containers (**apaga** os dados do banco e as filas) |
| `--project CAMINHO` | usa outra pasta de projeto |

---

## 5. Subir a API

Depois do Tutorial 1, a infraestrutura está de pé. Suba a API em um terminal e deixe aberto:

```bash
bun start
```

(ou, de uma vez só: `python3 setup_ambiente.py --start`)

Deve aparecer uma linha JSON com `"msg":"api listening"` e `"port":3000`. Confira em outro terminal:

```bash
curl http://localhost:3000/health/live
curl http://localhost:3000/health/ready
```

Resultado esperado: `{"status":"ok"}` e `{"status":"ready"}`.

---

## 6. Tutorial 2: rodar os testes (`script.py`)

O `script.py` executa o roteiro de testes completo (wallet, fluxo de apostas, reversões, referência fora de ordem, consultas, garantias do banco, fila SQS, concorrência e testes automatizados) e grava o resultado em `resultados_testes.txt`.

### Modo A: com a API já rodando (dois terminais)

Terminal 1:

```bash
python3 setup_ambiente.py --start
```

Terminal 2 (na mesma pasta):

```bash
python3 script.py
```

### Modo B: tudo em um comando

O script sobe a infraestrutura, as migrations, as filas e a API sozinho, roda os testes e encerra a API no final:

```bash
python3 script.py --setup
```

### Opções

| Opção | O que faz |
|---|---|
| `--setup` | prepara o ambiente e sobe/encerra a API por conta própria |
| `--failures` | inclui os testes que derrubam o Postgres e o LocalStack e os religam |
| `--crash` | inclui o teste de morte do processo entre commit e ack (exige `--setup`) |
| `--bases A,B,C` | usa várias instâncias no teste de concorrência (ex.: `http://localhost:3000,http://localhost:3001,http://localhost:3002`) |
| `--base URL` | endereço da API (padrão `http://localhost:3000`) |
| `--out ARQUIVO` | arquivo de saída (padrão `resultados_testes.txt`) |

Execução completa:

```bash
python3 script.py --setup --failures --crash
```

### O resultado

O terminal mostra o andamento (`[PASS]`, `[FAIL]` ou `[SKIP]` por teste) e o relatório completo fica em **`resultados_testes.txt`**, na pasta do projeto. Exemplo:

```
[PASS] A1 - Aposta de 25,00
    Esperado: HTTP 200 PROCESSED saldo 75.00 replay=False
    Obtido  : HTTP 200; status=PROCESSED; erro/motivo=None; saldo=75.00; replay=False

[FAIL] A14 - Worker retoma a devolucao pendente
    Esperado: tx-11 PROCESSED, kind REFUND, saldo 90.00 (ate 40s)
    Obtido  : sem resposta PROCESSED em 40s

======================================================================
RESUMO: 70 verificacoes | PASS 69 | FAIL 1 | SKIP 0
```

- **PASS**: o resultado bateu com o esperado.
- **FAIL**: não bateu. O `.txt` mostra o esperado e o obtido lado a lado.
- **SKIP**: o teste foi pulado (por exemplo, sem `--setup` o teste de logs não consegue ler o log da API).
- O script termina com código `0` se tudo passou e `1` se houve falha.

### O que o roteiro cobre

| Bloco | Testes |
|---|---|
| Saúde | `live` e `ready` |
| Wallet | criação, duplicada (409), valores inválidos (400) |
| Fluxo da wallet A | aposta, replay, conflito de chave, saldo insuficiente, ganho, LOSS, REFUND, ROLLBACK, reversão duplicada, referência fora de ordem retomada pelo worker, reconciliação, eventos do outbox |
| Regras e erros | reversão que deixaria saldo negativo, entradas inválidas, moeda, jogador e wallet que não batem |
| Consultas | por id, por provedor, extrato paginado, cursor inválido |
| Banco | ledger imutável, saldo negativo bloqueado, divergência de reconciliação, logs sem valores |
| Fila SQS | mensagem normal, duplicada, outro `messageId`, rejeição de negócio, DLQ |
| Concorrência | 50 requisições iguais em paralelo, duas apostas de 80 contra 100, várias instâncias, outbox drenado |
| Automáticos | `bun run typecheck` e `bun test src` |
| `--failures` | Postgres fora (503 com `Retry-After`), SQS fora e recuperação |
| `--crash` | morte entre commit e ack, redelivery sem débito duplicado |
| com `--setup` | desligamento gracioso (SIGTERM) e reinício com consistência |

> **Windows:** os testes de desligamento (`F1`), de reinício (`F4`) e o `--crash` dependem de sinais de processo e foram pensados para Linux/Mac. No Windows podem se comportar de forma diferente; se precisar deles, rode no WSL.

### Testes automatizados do próprio projeto

```bash
bun run typecheck
bun test src                       # 7 testes de unidade (não precisam de nada rodando)
BASE_URLS=http://localhost:3000 bun test test    # 6 testes de integração (precisam da API)
```

No PowerShell: `$env:BASE_URLS="http://localhost:3000"; bun test test`.

Para o passo a passo manual, comando a comando e com o resultado esperado de cada um, use o `GUIA_DE_TESTES.md`.

---

## 7. Rodar 3 instâncias no Docker

Para provar que o sistema é correto com várias instâncias, suba 3 cópias da API (portas 3000, 3001 e 3002):

```bash
docker compose up --build --scale api=3
```

Depois, em outro terminal:

```bash
python3 script.py --bases http://localhost:3000,http://localhost:3001,http://localhost:3002
```

O serviço `setup` do Compose roda as migrations e cria as filas uma única vez antes de as instâncias subirem.

---

## 8. Referência rápida

### Endpoints

| Método | Caminho | Para quê |
|---|---|---|
| `POST` | `/wallets` | criar wallet (cria o lançamento de abertura se houver saldo inicial) |
| `GET` | `/wallets/:walletId` | saldo e versão |
| `GET` | `/wallets/:walletId/ledger?cursor=&limit=` | extrato paginado (cursor opaco) |
| `POST` | `/wallets/:walletId/reconciliation` | compara saldo guardado com a soma do ledger |
| `POST` | `/wagering/transactions` | submeter operação (header `Idempotency-Key` obrigatório) |
| `GET` | `/wagering/transactions/:transactionId` | consultar por id interno |
| `GET` | `/providers/:providerId/wagering/transactions/:externalTransactionId` | consultar por id do provedor |
| `GET` | `/health/live`, `/health/ready`, `/metrics` | saúde e métricas |

### Status HTTP

| Status | Significado |
|---|---|
| 200 | transação `PROCESSED` (inclui replay) |
| 201 | wallet criada |
| 202 | aceita, mas `PENDING_REFERENCE` (aguardando a referência) |
| 400 | payload inválido ou header `Idempotency-Key` ausente |
| 404 | wallet ou transação não encontrada |
| 409 | conflito de idempotência ou wallet duplicada |
| 422 | rejeitada por regra de negócio (corpo traz `failureCode`) |
| 503 | falha transitória de infraestrutura (traz `Retry-After`; reenvie com a mesma chave) |

### Códigos de falha (`failureCode`)

`INSUFFICIENT_FUNDS`, `REVERSAL_WOULD_OVERDRAW`, `REFERENCE_NOT_FOUND`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`, `REFERENCE_KIND_INVALID`, `ALREADY_REVERSED`, `AMOUNT_MISMATCH`, `CURRENCY_MISMATCH`, `PLAYER_MISMATCH`.

### Exemplo: apostar

```bash
curl -s -X POST http://localhost:3000/wagering/transactions \
  -H 'content-type: application/json' -H 'Idempotency-Key: provider-a:tx-1' \
  -d '{"providerId":"provider-a","externalTransactionId":"tx-1","playerId":"<UUID>","walletId":"<UUID>","roundId":"r1","gameId":"g1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

---

## 9. Problemas comuns

| Sintoma | Causa e solução |
|---|---|
| "Docker não está em execução" | abra o Docker Desktop (Windows/Mac) ou inicie o serviço (`sudo systemctl start docker`) e rode de novo |
| "Docker Compose v2 não encontrado" | atualize o Docker; é preciso o comando `docker compose` (com espaço) |
| Falha ao subir os containers | as portas 5432 (Postgres) ou 4566 (LocalStack) estão ocupadas; pare o outro Postgres ou mude as portas no `docker-compose.yml` |
| "Bun não encontrado" | rode com `--install-bun`, ou instale em https://bun.sh. Depois de instalar, abra um terminal novo |
| `Nest can't resolve dependencies` ao subir a API | falta `@Inject(Classe)` em algum construtor (o Bun não gera os metadados de tipo) |
| O LocalStack não sobe | o projeto fixa a imagem `localstack/localstack:3.8`; versões mais novas podem exigir conta. Mantenha a 3.8 ou use o MiniStack |
| Os dados sumiram | o Compose não define volumes; `docker compose down` apaga banco e filas. Rode o `setup_ambiente.py` de novo |
| `connection refused` nos testes | a API não está rodando; suba com `bun start` ou use `script.py --setup` |
| Teste de SQS não debita | o consumidor está desligado (`CONSUMER_ENABLED=0`) ou as filas não existem (rode `bun run queues`) |
| Devolução fica `PENDING_REFERENCE` | o worker está desligado (`WORKERS_ENABLED=0`) ou o primeiro retry (5 s) ainda não passou |
| Acentos estranhos no Windows | use o Windows Terminal ou rode `chcp 65001` antes do script |

---

## 10. Limitações conhecidas

- A listagem do ledger usa a moeda `BRL` como padrão, porque a tabela de ledger não guarda `currency`. Para multimoeda, busque a moeda da wallet com um `JOIN`.
- No replay de uma mensagem da fila com **outro** `messageId`, a linha da inbox desse `messageId` fica com `processed_at` vazio (o saldo não é afetado). Para corrigir, execute o `UPDATE` da inbox também no ramo de replay do `ProcessWagerService.execute`.
- O consumidor SQS processa as mensagens em série por instância; o paralelismo vem de ter várias instâncias e vários grupos (um por wallet).
- A ordem global dos eventos entre vários publishers do outbox não é garantida; use `walletVersion` do evento para reordenar.
- O estado `FAILED` existe no modelo, mas nenhum fluxo atual o usa (erros permanentes vão para a DLQ).
- O trigger de imutabilidade do ledger não cobre `TRUNCATE`.
- Teste de carga (`bun run test:load`) e autenticação não foram implementados.
- O Compose não tem volumes: banco e filas não persistem entre `down` e `up`.

As decisões e trade-offs completos devem constar no `ARCHITECTURE.md`.