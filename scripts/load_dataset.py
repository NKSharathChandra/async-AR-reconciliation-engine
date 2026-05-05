"""Submit every row of docs/erp_export.csv to the workflow engine in parallel.

Usage (from repo root, with the API running):
    python scripts/load_dataset.py
    python scripts/load_dataset.py --limit 50 --concurrency 20
"""
import argparse
import asyncio
import csv
from pathlib import Path

import httpx

DEFAULT_CSV = Path(__file__).resolve().parent.parent / "docs" / "erp_export.csv"
NUMERIC_COLUMNS = {
    "Customer Balance",
    "Invoice Total", "Invoice applied amount", "Invoice exchange rate",
    "Payment Total", "Payment applied amount", "Payment exchange rate",
    "Credit Total", "Credit applied amount", "Credit exchange rate",
    "Adjustment Total", "Adjustment applied amount", "Adjustment exchange rate",
}


def coerce(row: dict) -> dict:
    return {k: (float(v) if k in NUMERIC_COLUMNS else v) for k, v in row.items()}


async def submit(client: httpx.AsyncClient, sem: asyncio.Semaphore, base_url: str, row: dict):
    async with sem:
        # Customer ID acts as the natural idempotency key — re-running the loader
        # is safe and won't spawn duplicate workflows.
        body = {"idempotency_key": row["Customer ID"], "payload": row}
        r = await client.post(f"{base_url}/api/workflows", json=body, timeout=30)
        r.raise_for_status()
        return r.json()["id"]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()

    with open(args.csv, newline="") as f:
        rows = [coerce(r) for r in csv.DictReader(f)]
    if args.limit:
        rows = rows[: args.limit]

    sem = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *(submit(client, sem, args.base_url, r) for r in rows),
            return_exceptions=True,
        )

    ok = sum(1 for r in results if not isinstance(r, Exception))
    print(f"Submitted {ok}/{len(rows)} records")
    for r in results:
        if isinstance(r, Exception):
            print(f"  error: {r}")
            break


if __name__ == "__main__":
    asyncio.run(main())
