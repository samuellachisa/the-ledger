#!/usr/bin/env python3
"""
generate_test_results.py

Generates `artifacts/test_results.txt` by running the full pytest suite.
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate artifacts/test_results.txt")
    parser.add_argument("--pytest-cmd", default=os.getenv("PYTEST_CMD", "pytest"))
    parser.add_argument("--artifacts-dir", default="artifacts")
    parser.add_argument("--tests", default="tests/")
    args = parser.parse_args()

    out_dir = Path(args.artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "test_results.txt"

    cmd = [
        args.pytest_cmd,
        args.tests,
        "-v",
        "--tb=short",
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    out_path.write_text(proc.stdout + ("\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")

    if proc.returncode != 0:
        raise SystemExit(f"Test run failed with exit code {proc.returncode}")

    print(f"✓ Wrote {out_path}")


if __name__ == "__main__":
    main()

