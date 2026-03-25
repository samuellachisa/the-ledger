#!/usr/bin/env python3
"""
generate_cost_report.py

Generates `artifacts/api_cost_report.txt` by summing the per-node costs tracked
in `AgentNodeExecuted.llm_cost_usd`.

This script is intentionally simple: Week 5 agents already calculate
`llm_cost_usd` during node execution (even if LLM calls are stubbed).
"""

from __future__ import annotations

import argparse
import asyncio
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

import asyncpg


async def _run(db_url: str, artifacts_dir: str) -> None:
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=6)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                  COALESCE(payload->>'node_name', '') AS node_name,
                  COALESCE((payload->>'llm_cost_usd')::numeric, 0) AS llm_cost_usd
                FROM events
                WHERE event_type = 'AgentNodeExecuted'
                """
            )

        total = Decimal("0")
        by_node: Dict[str, Decimal] = {}
        for r in rows:
            node = r["node_name"] or "unknown"
            cost = r["llm_cost_usd"] if r["llm_cost_usd"] is not None else Decimal("0")
            total += cost
            by_node[node] = by_node.get(node, Decimal("0")) + cost

        # Sort by cost descending.
        breakdown = sorted(by_node.items(), key=lambda kv: kv[1], reverse=True)

        Path(artifacts_dir).mkdir(parents=True, exist_ok=True)
        out_path = Path(artifacts_dir) / "api_cost_report.txt"

        with out_path.open("w", encoding="utf-8") as f:
            f.write("API Cost Report\n")
            f.write("===============\n\n")
            f.write(f"Total API Cost: ${float(total):.2f}\n\n")
            if float(total) > 50.0:
                f.write("WARNING: Cost exceeds $50 budget.\n\n")
            f.write("Cost by Node:\n")
            f.write("-------------\n")
            for node, cost in breakdown[:30]:
                f.write(f"  {node[:60]:<60} ${float(cost):.2f}\n")

        print(f"✓ Wrote {out_path}")

    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate api_cost_report.txt")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"), required=False)
    parser.add_argument("--artifacts-dir", default="artifacts")
    args = parser.parse_args()

    if not args.db_url:
        raise SystemExit("ERROR: Provide --db-url or set DATABASE_URL.")

    asyncio.run(_run(args.db_url, args.artifacts_dir))


if __name__ == "__main__":
    main()

