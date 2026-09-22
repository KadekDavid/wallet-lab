"""
The test that actually teaches you something.

Run it against the Go service and watch two things fail if the wallet is wrong:

  Scenario A — retry storm
      Fire the SAME Idempotency-Key 50 times concurrently.
      Correct behaviour: money moves ONCE. 49 responses come back replayed=true.

  Scenario B — double spend
      Account holds 100. Fire 10 concurrent bets of 100 each, all with
      DIFFERENT keys.
      Correct behaviour: exactly ONE succeeds, nine get 422. Balance ends at 0,
      never negative.

If you remove `FOR UPDATE` from the Go code and re-run scenario B, you will see
several bets succeed and the balance go negative. Do that once — seeing it break
is worth more than reading about it.

    pip install httpx
    python loadtest.py
"""

from __future__ import annotations

import asyncio
import uuid

import httpx

BASE = "http://localhost:8080"


async def create_account(client: httpx.AsyncClient, owner: str) -> int:
    r = await client.post(f"{BASE}/accounts", json={"owner": owner})
    r.raise_for_status()
    return r.json()["id"]


async def balance(client: httpx.AsyncClient, account_id: int) -> int:
    r = await client.get(f"{BASE}/accounts/{account_id}")
    r.raise_for_status()
    return r.json()["balance"]


async def post_tx(
    client: httpx.AsyncClient, key: str, account_id: int, amount: int, kind: str
) -> httpx.Response:
    return await client.post(
        f"{BASE}/transactions",
        headers={"Idempotency-Key": key},
        json={
            "account_id": account_id,
            "amount": amount,
            "kind": kind,
            "reference": f"round-{key[:8]}",
        },
    )


async def scenario_a_retry_storm(client: httpx.AsyncClient) -> None:
    print("\n=== A. retry storm: one key, 50 concurrent sends ===")
    acct = await create_account(client, "retry-tester")
    await post_tx(client, str(uuid.uuid4()), acct, 100_000, "deposit")

    key = str(uuid.uuid4())
    results = await asyncio.gather(
        *[post_tx(client, key, acct, 10_000, "bet") for _ in range(50)]
    )

    ok = [r for r in results if r.status_code == 200]
    replayed = [r for r in ok if r.json().get("replayed")]
    final = await balance(client, acct)

    print(f"  200 responses     : {len(ok)}/50")
    print(f"  marked replayed   : {len(replayed)}")
    print(f"  balance           : {final}  (expected 90000)")
    print("  PASS" if final == 90_000 else "  FAIL — money moved more than once")


async def scenario_b_double_spend(client: httpx.AsyncClient) -> None:
    print("\n=== B. double spend: 10 concurrent bets, balance only covers one ===")
    acct = await create_account(client, "race-tester")
    await post_tx(client, str(uuid.uuid4()), acct, 100, "deposit")

    results = await asyncio.gather(
        *[post_tx(client, str(uuid.uuid4()), acct, 100, "bet") for _ in range(10)]
    )

    accepted = [r for r in results if r.status_code == 200]
    rejected = [r for r in results if r.status_code == 422]
    final = await balance(client, acct)

    print(f"  accepted          : {len(accepted)}  (expected 1)")
    print(f"  rejected (422)    : {len(rejected)}  (expected 9)")
    print(f"  balance           : {final}  (expected 0, must never be negative)")
    print("  PASS" if len(accepted) == 1 and final == 0 else "  FAIL — race condition")


async def main() -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        await scenario_a_retry_storm(client)
        await scenario_b_double_spend(client)


if __name__ == "__main__":
    asyncio.run(main())
