"""
Reconciliation service.

This is the Python half, and it is deliberately the half that plays to what you
already know: taking raw rows, aggregating them, and proving that two sources of
truth agree. It is the same shape as the BPJS equalisation work — raw data in,
pivot, compare, report the differences.

Two invariants are checked:

  1. Every transaction's ledger entries sum to zero.
     If they don't, money was created or destroyed.

  2. Every account's cached `balance` equals the sum of its ledger entries.
     If it doesn't, a write path is buggy.

In production this runs on a schedule and pages someone when it fails.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import asyncpg
from fastapi import FastAPI

DSN = "postgres://wallet:wallet@localhost:5432/wallet"

app = FastAPI(title="Wallet Reconciler")


@dataclass
class Mismatch:
    account_id: int
    owner: str
    cached_balance: int
    ledger_balance: int

    @property
    def difference(self) -> int:
        return self.cached_balance - self.ledger_balance


async def _connect() -> asyncpg.Connection:
    return await asyncpg.connect(DSN)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/reconcile/balances")
async def reconcile_balances() -> dict:
    """Compare each account's cached balance against its ledger entries."""
    conn = await _connect()
    try:
        rows = await conn.fetch(
            """
            SELECT a.id,
                   a.owner,
                   a.balance                       AS cached_balance,
                   COALESCE(SUM(e.amount), 0)      AS ledger_balance
            FROM accounts a
            LEFT JOIN ledger_entries e ON e.account_id = a.id
            GROUP BY a.id, a.owner, a.balance
            ORDER BY a.id
            """
        )
    finally:
        await conn.close()

    mismatches = [
        Mismatch(
            account_id=r["id"],
            owner=r["owner"],
            cached_balance=r["cached_balance"],
            ledger_balance=r["ledger_balance"],
        )
        for r in rows
        if r["cached_balance"] != r["ledger_balance"]
    ]

    return {
        "accounts_checked": len(rows),
        "balanced": not mismatches,
        "mismatches": [
            {
                "account_id": m.account_id,
                "owner": m.owner,
                "cached_balance": m.cached_balance,
                "ledger_balance": m.ledger_balance,
                "difference": m.difference,
            }
            for m in mismatches
        ],
    }


@app.get("/reconcile/transactions")
async def reconcile_transactions() -> dict:
    """Every transaction's entries must sum to exactly zero."""
    conn = await _connect()
    try:
        rows = await conn.fetch(
            """
            SELECT t.id, t.kind, t.reference, SUM(e.amount) AS total
            FROM transactions t
            JOIN ledger_entries e ON e.transaction_id = t.id
            GROUP BY t.id, t.kind, t.reference
            HAVING SUM(e.amount) <> 0
            ORDER BY t.id
            """
        )
        total = await conn.fetchval("SELECT count(*) FROM transactions")
    finally:
        await conn.close()

    return {
        "transactions_checked": total,
        "balanced": not rows,
        "unbalanced": [
            {
                "transaction_id": r["id"],
                "kind": r["kind"],
                "reference": r["reference"],
                "sum": r["total"],
            }
            for r in rows
        ],
    }


@app.get("/report/daily")
async def daily_report() -> dict:
    """Turnover and house result by transaction kind — the operator's daily view."""
    conn = await _connect()
    try:
        rows = await conn.fetch(
            """
            SELECT t.kind,
                   count(*)                                    AS transactions,
                   SUM(CASE WHEN e.amount < 0 THEN -e.amount
                            ELSE 0 END)                        AS debited
            FROM transactions t
            JOIN ledger_entries e ON e.transaction_id = t.id
            JOIN accounts a       ON a.id = e.account_id AND a.kind = 'player'
            WHERE t.created_at >= date_trunc('day', now())
            GROUP BY t.kind
            ORDER BY t.kind
            """
        )
    finally:
        await conn.close()

    return {
        "date": "today",
        "by_kind": [
            {"kind": r["kind"], "transactions": r["transactions"], "debited": r["debited"]}
            for r in rows
        ],
    }
