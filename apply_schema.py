"""Apply src/schema.sql to apex_ledger database."""
import psycopg2
import os
import sys

db_url = sys.argv[1] if len(sys.argv) > 1 else "postgresql://postgres:12345@localhost/apex_ledger"
schema_file = os.path.join(os.path.dirname(__file__), "src", "schema.sql")

conn = psycopg2.connect(db_url)
conn.autocommit = True
with open(schema_file) as f:
    sql = f.read()
with conn.cursor() as cur:
    cur.execute(sql)
conn.close()
print(f"Schema applied to {db_url}")
