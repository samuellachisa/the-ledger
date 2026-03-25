#!/usr/bin/env python3
"""
generate_projection_lag_report.py

Runs the projection daemon lag SLO test and stores output in:
  artifacts/projection_lag_report.txt
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate projection lag report")
    parser.add_argument("--pytest-cmd", default=os.getenv("PYTEST_CMD", "pytest"))
    parser.add_argument("--test-arg", default="tests/test_projections.py::test_projection_daemon_lag_slo_under_load")
    parser.add_argument("--artifacts-dir", default="artifacts")
    args = parser.parse_args()

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    out_path = artifacts_dir / "projection_lag_report.txt"

    cmd = [
        args.pytest_cmd,
        args.test_arg,
        "-v",
        "-s",
        "--tb=short",
    ]

    print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out_path.write_text(proc.stdout + ("\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")

    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        raise SystemExit(f"Projection lag report failed with exit code {proc.returncode}")

    print(f"✓ Wrote {out_path}")


if __name__ == "__main__":
    main()

