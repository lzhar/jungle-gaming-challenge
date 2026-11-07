import { Migration } from '@mikro-orm/migrations';

export class Migration20260101000000 extends Migration {
  async up(): Promise<void> {
    this.addSql(`
      CREATE TABLE wallets (
        id uuid PRIMARY KEY,
        player_id uuid NOT NULL,
        currency char(3) NOT NULL,
        balance numeric(20,2) NOT NULL CHECK (balance >= 0),
        version integer NOT NULL CHECK (version >= 1),
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        UNIQUE (player_id, currency)
      );

      CREATE TABLE wager_transactions (
        id uuid PRIMARY KEY,
        provider_id text NOT NULL,
        external_transaction_id text NOT NULL,
        idempotency_key text NOT NULL UNIQUE,
        payload_hash text NOT NULL,
        wallet_id uuid NOT NULL REFERENCES wallets(id),
        player_id uuid NOT NULL,
        round_id text NOT NULL,
        game_id text NOT NULL,
        kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        amount numeric(20,2) NOT NULL CHECK (amount > 0),
        currency char(3) NOT NULL,
        reference_external_transaction_id text,
        reference_transaction_id uuid REFERENCES wager_transactions(id),
        status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        failure_code text,
        balance_after numeric(20,2),
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        created_at timestamptz NOT NULL,
        processed_at timestamptz,
        UNIQUE (provider_id, external_transaction_id),
        CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
        CHECK (status NOT IN ('REJECTED','FAILED') OR failure_code IS NOT NULL)
      );

      CREATE UNIQUE INDEX uq_single_reversal ON wager_transactions (reference_transaction_id)
        WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
      CREATE INDEX ix_pending_reference ON wager_transactions (next_attempt_at)
        WHERE status = 'PENDING_REFERENCE';

      CREATE TABLE wallet_ledger_entries (
        seq bigserial UNIQUE,
        id uuid PRIMARY KEY,
        wallet_id uuid NOT NULL REFERENCES wallets(id),
        transaction_id uuid NOT NULL REFERENCES wager_transactions(id),
        direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
        amount numeric(20,2) NOT NULL CHECK (amount > 0),
        balance_before numeric(20,2) NOT NULL CHECK (balance_before >= 0),
        balance_after numeric(20,2) NOT NULL CHECK (balance_after >= 0),
        created_at timestamptz NOT NULL,
        UNIQUE (wallet_id, transaction_id),
        CHECK (
          (direction = 'CREDIT' AND balance_after = balance_before + amount) OR
          (direction = 'DEBIT' AND balance_after = balance_before - amount)
        )
      );
      CREATE INDEX ix_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

      CREATE FUNCTION forbid_ledger_mutation() RETURNS trigger AS $$
      BEGIN RAISE EXCEPTION 'wallet_ledger_entries is append-only'; END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger_entries
        FOR EACH ROW EXECUTE FUNCTION forbid_ledger_mutation();

      CREATE TABLE inbox_messages (
        consumer_name text NOT NULL,
        message_id text NOT NULL,
        payload_hash text NOT NULL,
        received_at timestamptz NOT NULL,
        processed_at timestamptz,
        PRIMARY KEY (consumer_name, message_id)
      );

      CREATE TABLE outbox_messages (
        id uuid PRIMARY KEY,
        aggregate_id text NOT NULL,
        event_type text NOT NULL,
        payload jsonb NOT NULL,
        occurred_at timestamptz NOT NULL,
        attempts integer NOT NULL DEFAULT 0,
        next_attempt_at timestamptz,
        published_at timestamptz
      );
      CREATE INDEX ix_outbox_pending ON outbox_messages (occurred_at) WHERE published_at IS NULL;
    `);
  }

  async down(): Promise<void> {
    this.addSql(`
      DROP TABLE outbox_messages;
      DROP TABLE inbox_messages;
      DROP TRIGGER ledger_immutable ON wallet_ledger_entries;
      DROP FUNCTION forbid_ledger_mutation();
      DROP TABLE wallet_ledger_entries;
      DROP TABLE wager_transactions;
      DROP TABLE wallets;
    `);
  }
}