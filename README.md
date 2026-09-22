# Wallet Lab

A small transactional backend built to work through the failure modes that
actually matter in any system where money moves: a **ledger/wallet service in
Go** and a **reconciliation service in Python**, backed by Postgres.

The service is domain-agnostic — the same shape applies to payments, fintech
balances, or in-game currency. What it demonstrates is not a CRUD API but the
three properties a money-moving endpoint cannot ship without:

| Property | Enforced by | Fails without it |
|---|---|---|
| **Idempotency** | `idempotency_keys` unique index + replay logic in `execute()` | A retried HTTP request debits the account twice |
| **Atomicity** | one Postgres transaction per money movement | Ledger and cached balance disagree |
| **Concurrency safety** | `SELECT ... FOR UPDATE` row lock | Two concurrent bets both pass the balance check → double spend |
| **Verifiability** | the Python reconciler | Nobody notices when the first three fail |

---

## Architecture

```
┌─────────────┐        ┌──────────────┐        ┌───────────────────┐
│  loadtest.py │──────▶│  go-wallet    │──────▶│  Postgres          │
│  (httpx,     │        │  :8080        │        │  accounts          │
│  asyncio)    │        │  (net/http)   │        │  transactions       │
└─────────────┘        └──────────────┘        │  ledger_entries     │
                                                 │  idempotency_keys   │
┌─────────────┐        ┌──────────────┐        └───────────────────┘
│  operator /  │──────▶│  python-recon │◀──────────────┘
│  interviewer │        │  :8000        │
└─────────────┘        │  (FastAPI)    │
                        └──────────────┘
```

**go-wallet** owns writes. Every balance change happens inside a single
database transaction via `Server.execute()` (`go-wallet/main.go`):

```
BEGIN
  claim the idempotency key      -- unique index does the real work
  lock the account row           -- SELECT ... FOR UPDATE
  check funds
  write the double-entry (ledger_entries, sum == 0)
  update the cached balance
  store the response for replay
COMMIT
```

**python-recon** owns proof. It never writes — it only checks that the
cached `balance` on `accounts` still agrees with the sum of `ledger_entries`,
and that every transaction's entries sum to exactly zero.

---

## The two concepts this project is built to demonstrate

### Idempotency

Clients retry. A mobile app that times out on a bet request will send it
again; a load balancer failover can duplicate a request in flight. If the
same `POST /transactions` is processed twice, the account gets debited twice
for a single real-world event.

The fix here is not "check if it looks like a duplicate" — it's a database
constraint. Every request carries an `Idempotency-Key` header. `execute()`
does an `INSERT ... ON CONFLICT (key) DO UPDATE SET key = key RETURNING
request_hash, response_body` as the *first* statement in the transaction:

- **New key** → the insert succeeds with no stored response, so the request
  proceeds normally.
- **Same key, same body** (a genuine retry) → the previous response is
  already stored; it's replayed verbatim and no money moves again.
- **Same key, different body** → rejected with `409 Conflict`. Reusing a key
  for a different payload is a caller bug and must never silently succeed.

The uniqueness guarantee comes from the Postgres `PRIMARY KEY` on
`idempotency_keys.key` (`go-wallet/schema.sql`), not from application logic —
so it holds even under concurrent retries of the same key.

### Row-level locking (pessimistic concurrency control)

Two concurrent requests against the same account both read `balance = 100`
before either has written anything. Both compute "100 - 100 ≥ 0" and both
proceed. The account ends up debited 200 from a balance of 100.

`SELECT balance FROM accounts WHERE id = $1 FOR UPDATE` closes this window:
the first transaction to reach that line takes an exclusive row lock, and any
other transaction touching the same row blocks until the first one commits or
rolls back. By the time the second transaction reads the balance, it sees the
first transaction's result — not the stale value from before it started.

This is pessimistic locking: correct, and simple to reason about, at the cost
of serializing all writes to one account. (`schema.sql` also carries an
unused `version` column for the optimistic-locking alternative — compare-and-swap
on `version` instead of holding a lock — as a documented next step, not
implemented here.)

**I verified this isn't just theoretical** by temporarily removing `FOR
UPDATE` from `main.go` and re-running the load test — see results below.

---

## Running it

Postgres's default port (5432) is often already taken by a local install; if
`docker compose up` succeeds but the Go service reports `role "wallet" does
not exist`, something else is listening on 5432 — check with
`lsof -iTCP:5432 -sTCP:LISTEN` and remap the container port (e.g. `"5433:5432"`
in `docker-compose.yml`, with `DATABASE_URL` updated to match) rather than
touching whatever's already running.

```bash
docker compose up -d db          # Postgres 16; schema.sql loads automatically

cd go-wallet
go mod tidy                      # fetches lib/pq
go run .                         # listens on :8080

cd ../python-recon
pip install -r requirements.txt
uvicorn recon:app --port 8000    # reconciler, listens on :8000
```

Then, from `python-recon/`:

```bash
pip install httpx
python loadtest.py
```

---

## Load test results

`loadtest.py` runs two scenarios against the live Go service over real HTTP,
using `asyncio.gather` to fire genuinely concurrent requests.

### Scenario A — retry storm

The same `Idempotency-Key` sent 50 times concurrently against a freshly
funded account. Correct behaviour: money moves exactly once; the other 49
responses replay the first result.

```
=== A. retry storm: one key, 50 concurrent sends ===
  200 responses     : 50/50
  marked replayed   : 49
  balance           : 90000  (expected 90000)
  PASS
```

### Scenario B — double spend, with the row lock in place

An account holding 100 is hit with 10 concurrent bets of 100 each, each with
a distinct idempotency key. Correct behaviour: exactly one succeeds, nine are
rejected with `422 Insufficient Funds`, and the balance lands at exactly 0.

```
=== B. double spend: 10 concurrent bets, balance only covers one ===
  accepted          : 1  (expected 1)
  rejected (422)    : 9  (expected 9)
  balance           : 0  (expected 0, must never be negative)
  PASS
```

Run five times back to back, this passed 5/5 — the lock makes the outcome
deterministic, not just usually-correct.

### Scenario B — with `FOR UPDATE` removed

Same test, same code, one line deleted (`main.go`, the `SELECT balance`
query). Because the race window is a handful of milliseconds, the failure
doesn't reproduce on every run on localhost — it took two attempts:

```
=== B. double spend: 10 concurrent bets, balance only covers one ===
  accepted          : 3  (expected 1)
  rejected (422)    : 7  (expected 9)
  balance           : -200  (expected 0, must never be negative)
  FAIL — race condition
```

Three concurrent transactions each read the balance before any of them wrote
their update, each independently decided 100 covers a 100 bet, and all three
committed. The account went negative — a real double spend, produced by
deleting one clause. `FOR UPDATE` was restored immediately after and
re-verified at 5/5 passes.

### Reconciliation, after all of the above

With the lock restored, `python-recon`'s invariants held across every account
and transaction touched by both scenarios and all their reruns:

```json
GET /reconcile/balances
{"accounts_checked": 15, "balanced": true, "mismatches": []}

GET /reconcile/transactions
{"transactions_checked": 30, "balanced": true, "unbalanced": []}
```

Every cached balance matched its ledger, and every transaction's double-entry
rows summed to zero — including the deposits and bets left behind by the
retry-storm and double-spend runs.

---

## A schema bug found while producing these results

`schema.sql` seeds a house account with an explicit `id = 1` but never
advances the `accounts` `BIGSERIAL` sequence to match. The very first
`POST /accounts` on a freshly initialised database collides with that seed
row (`duplicate key value violates unique constraint "accounts_pkey"`). Fixed
with one line after the seed insert:

```sql
SELECT setval('accounts_id_seq', (SELECT MAX(id) FROM accounts));
```

Worth mentioning here rather than quietly patching it, since manually seeding
a primary key that a serial column also generates is a mistake that shows up
in real schemas too — and the fix is a good instinct to have on hand.

---

## Project layout

```
wallet-lab/
├── docker-compose.yml       # Postgres 16, schema auto-loaded on first boot
├── go-wallet/
│   ├── main.go              # HTTP API + execute() — the transactional core
│   ├── schema.sql           # double-entry ledger schema
│   └── go.mod / go.sum
└── python-recon/
    ├── recon.py             # FastAPI reconciler: balance & ledger invariants
    ├── loadtest.py          # concurrency test harness (asyncio + httpx)
    └── requirements.txt
```

---

## What I'd build next

- Replace the `FOR UPDATE` row lock with optimistic concurrency on the
  existing `version` column, and compare the two under the same load test.
- Add a `rollback` transaction kind that reverses a prior transaction by
  reference, idempotently, refusing to reverse the same transaction twice.
- Run the reconciler on a schedule and expose the two invariant checks as
  Prometheus metrics instead of on-demand JSON.
- Thread a request ID through both services for correlated structured
  logging.
