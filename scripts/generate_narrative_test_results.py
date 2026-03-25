#!/usr/bin/env python3
"""
generate_narrative_test_results.py

Generates `artifacts/narrative_test_results.txt` by running:
  pytest tests/test_narratives.py -v --tb=long
"""

from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate narrative test artifact")
    parser.add_argument("--pytest-cmd", default=os.getenv("PYTEST_CMD", "pytest"))
    parser.add_argument("--artifacts-dir", default="artifacts")
    args = parser.parse_args()

    out_dir = Path(args.artifacts_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "narrative_test_results.txt"

    cmd = [
        args.pytest_cmd,
        "tests/test_narratives.py",
        "-v",
        "--tb=long",
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)
    out_path.write_text(proc.stdout + ("\n" + proc.stderr if proc.stderr else ""), encoding="utf-8")

    if proc.returncode != 0:
        raise SystemExit(f"Narrative tests failed with exit code {proc.returncode}")

    print(f"✓ Wrote {out_path}")


if __name__ == "__main__":
    main()

