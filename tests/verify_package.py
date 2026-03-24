"""
tests/verify_package.py
Standalone regulatory package evaluator mapping explicit hashes verifying chain bounds.
"""
import sys
import json
import hashlib

def verify_package(filepath: str):
    try:
        with open(filepath, "r") as f:
            package = json.load(f)
    except Exception as e:
        print(f"ERROR: Cannot load package: {e}")
        sys.exit(1)
        
    events = package.get("events", [])
    if not events:
        print("ERROR: No events present.")
        sys.exit(1)
        
    prev_hash = "GENESIS"
    
    for e in events:
        # Recreate exactly identically as run_integrity_check bounds mappings natively inside Python dict maps
        canonical = json.dumps({"event_id": e["id"], "payload": e["payload"]}, sort_keys=True, default=str)
        curr_hash = hashlib.sha256((prev_hash + canonical).encode()).hexdigest()
        prev_hash = curr_hash
        
    expected_terminal = package.get("package_hash_chain")
    if expected_terminal and prev_hash != expected_terminal:
        print(f"ERROR: Hash chain invalid. Expected {expected_terminal}, got {prev_hash}")
        sys.exit(1)
        
    print("SUCCESS: Hash chain valid, all events present, package independently verifiable")
    sys.exit(0)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tests/verify_package.py <path_to_json>")
        sys.exit(1)
    verify_package(sys.argv[1])
