#!/usr/bin/env python3
"""
run_concurrent.py

OCC collision load generator for producing `artifacts/occ_collision_report.txt`.

Notes:
- This script is meant for diagnostics (best-effort).
- It generates OCC conflicts by intentionally appending with stale expected_version.
- On conflicts, it performs a bounded exponential-backoff retry budget.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from typing import Callable, Dict, List, Tuple
from uuid import UUID, uuid5, uuid4, NAMESPACE_DNS

import asyncpg

from src.event_store import EventStore
from src.upcasting.registry import UpcasterRegistry
from ledger.schema.events import LoanApplicationSubmitted, OptimisticConcurrencyError


def _det_uuid(key: str) -> UUID:
    return uuid5(NAMESPACE_DNS, key)


async def _append_with_retry_budget(
    store: EventStore,
    aggregate_type: str,
    aggregate_id: UUID,
    make_events: Callable[[], List[object]],
    *,
    max_attempts: int = 5,
    base_ms: int = 10,
    factor: int = 2,
) -> Tuple[int, bool]:
    """
    Returns (retry_count, succeeded).

    retry_count counts how many OCC retries were needed beyond the first attempt.
    """
    last_err = None
    retries = 0
    for attempt in range(max_attempts):
        if attempt > 0:
            delay_ms = base_ms * (factor ** (attempt - 1))
            await asyncio.sleep(delay_ms / 1000)
            retries += 1

        expected_version = await store.stream_version(aggregate_type, aggregate_id)
        try:
            events = make_events()
            await store.append(aggregate_type, aggregate_id, events, expected_version)
            return retries, True
        except OptimisticConcurrencyError as e:
            last_err = e
            continue

    # Exhausted retries
    return retries, False


async def _run_application(
    store: EventStore,
    app_key: str,
    concurrency: int,
    rng: random.Random,
) -> Dict[str, object]:
    """
    Create 1 initial event, then concurrently attempt to append 1 event/task
    all using the same expected_version=1 to provoke conflicts.
    """
    aggregate_id = _det_uuid(app_key)
    aggregate_type = "LoanApplication"

    # Seed the stream with exactly one event so the stale expected_version is 1.
    seed_event = LoanApplicationSubmitted(
        applicant_name="seed",
        loan_amount=1000.0,
        loan_purpose="seed",
        applicant_id=_det_uuid(app_key + "-seed-applicant"),
        submitted_by="load_gen",
    )
    await store.append(aggregate_type, aggregate_id, [seed_event], expected_version=0)

    expected_version_stale = 1
    retry_distribution: Counter[int] = Counter()
    conflict_detected = 0
    conflict_resolved = 0
    conflict_unresolved = 0

    async def worker(worker_idx: int) -> None:
        nonlocal conflict_detected, conflict_resolved, conflict_unresolved

        make_events = lambda: [
            LoanApplicationSubmitted(
                applicant_name=f"worker-{worker_idx}",
                loan_amount=1000.0 + worker_idx,
                loan_purpose="load_test",
                applicant_id=_det_uuid(app_key + f"-w{worker_idx}"),
                submitted_by="load_gen",
            )
        ]

        try:
            await store.append(aggregate_type, aggregate_id, make_events(), expected_version_stale)
            return
        except OptimisticConcurrencyError:
            conflict_detected += 1
            retries, ok = await _append_with_retry_budget(
                store,
                aggregate_type,
                aggregate_id,
                make_events,
                max_attempts=5,
            )
            if ok:
                conflict_resolved += 1
                retry_distribution[retries] += 1
            else:
                conflict_unresolved += 1

    await asyncio.gather(*(worker(i) for i in range(concurrency)))

    return {
        "conflict_detected": conflict_detected,
        "conflict_resolved": conflict_resolved,
        "conflict_unresolved": conflict_unresolved,
        "retry_distribution": dict(retry_distribution),
    }


async def _run(applications: int, concurrency: int, db_url: str) -> None:
    pool = await asyncpg.create_pool(db_url, min_size=2, max_size=8)
    try:
        store = EventStore(pool, UpcasterRegistry())

        rng = random.Random(42)
        start = time.time()

        tasks = []
        for i in range(applications):
            app_key = f"OCC-{i+1:03d}"
            tasks.append(_run_application(store, app_key, concurrency, rng))

        results = await asyncio.gather(*tasks)

        duration_s = time.time() - start
        occ_conflicts_detected = sum(int(r["conflict_detected"]) for r in results)
        occ_conflicts_resolved = sum(int(r["conflict_resolved"]) for r in results)
        occ_conflicts_unresolved = sum(int(r["conflict_unresolved"]) for r in results)

        retry_hist: Counter[int] = Counter()
        for r in results:
            retry_hist.update(r.get("retry_distribution", {}))

        print("Load Generator Report")
        print("=====================")
        print(f"Applications: {applications}")
        print(f"Concurrency: {concurrency} workers")
        print(f"Duration: {duration_s:.1f}s")
        print("")
        print(f"OCC Conflicts Detected: {occ_conflicts_detected}")
        print(f"OCC Conflicts Resolved: {occ_conflicts_resolved} (100% if unresolved=0)")
        print(f"Unresolved Conflicts: {occ_conflicts_unresolved}")
        print("")
        print("Retry Distribution:")
        for retries, count in sorted(retry_hist.items(), key=lambda kv: kv[0]):
            print(f"  {retries} retries: {count} conflicts")
        print("")
        print("✓ Done")
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate OCC collision report")
    parser.add_argument("--applications", type=int, default=15)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"), help="PostgreSQL URL")
    args = parser.parse_args()

    if not args.db_url:
        raise SystemExit("ERROR: Provide --db-url or set DATABASE_URL.")

    asyncio.run(_run(args.applications, args.concurrency, args.db_url))


if __name__ == "__main__":
    main()

