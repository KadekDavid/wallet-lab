-- Double-entry ledger. Money is never "edited" — it only moves between accounts.
-- Every transaction writes N entries that MUST sum to zero.

CREATE TABLE IF NOT EXISTS accounts (
    id          BIGSERIAL PRIMARY KEY,
    owner       TEXT        NOT NULL,
    kind        TEXT        NOT NULL,          -- 'player' | 'house'
    currency    CHAR(3)     NOT NULL DEFAULT 'IDR',
    -- balance is a cached projection of ledger_entries; the reconciler proves it correct
    balance     BIGINT      NOT NULL DEFAULT 0, -- minor units (cents). NEVER use float for money.
    version     BIGINT      NOT NULL DEFAULT 0, -- optimistic locking
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    id          BIGSERIAL   PRIMARY KEY,
    kind        TEXT        NOT NULL,          -- 'deposit' | 'bet' | 'win'
    reference   TEXT        NOT NULL,          -- caller's round/order id
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id             BIGSERIAL   PRIMARY KEY,
    transaction_id BIGINT      NOT NULL REFERENCES transactions(id),
    account_id     BIGINT      NOT NULL REFERENCES accounts(id),
    amount         BIGINT      NOT NULL,       -- signed: debit negative, credit positive
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_entries_account ON ledger_entries(account_id);

-- The idempotency table is what stops a retried HTTP request from moving money twice.
-- UNIQUE on key is the actual guarantee — not application logic.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key            TEXT        PRIMARY KEY,
    request_hash   TEXT        NOT NULL,       -- detects same key + different body
    response_body  JSONB,
    transaction_id BIGINT      REFERENCES transactions(id),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed a house account so bets have somewhere to go.
INSERT INTO accounts (id, owner, kind, balance)
VALUES (1, 'house', 'house', 0)
ON CONFLICT (id) DO NOTHING;

-- The explicit id above does not advance the BIGSERIAL sequence, so the next
-- INSERT ... RETURNING id would collide with the house account. Bump it.
SELECT setval('accounts_id_seq', (SELECT MAX(id) FROM accounts));
