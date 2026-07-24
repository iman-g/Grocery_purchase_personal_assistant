"""Quick health check on grocery.duckdb — run: python warehouse/inspect.py"""
from pathlib import Path
import duckdb

DB = Path(__file__).resolve().parent.parent / "grocery.duckdb"
if not DB.exists():
    raise SystemExit(f"No database at {DB} — run `python warehouse/load.py` first.")

con = duckdb.connect(str(DB), read_only=True)

print("=== Tables in raw schema ===")
for (name,) in con.execute(
    "SELECT table_name FROM information_schema.tables WHERE table_schema='raw' ORDER BY 1"
).fetchall():
    n = con.execute(f"SELECT count(*) FROM raw.{name}").fetchone()[0]
    print(f"  raw.{name}: {n:,} rows")

print("\n=== AH price snapshots per day ===")
for d, c in con.execute(
    "SELECT source_date, count(*) FROM raw.raw_ah_products GROUP BY 1 ORDER BY 1"
).fetchall():
    print(f"  {d}: {c:,}")

print("\n=== Lidl offers per day ===")
for d, c in con.execute(
    "SELECT source_date, count(*) FROM raw.raw_lidl_offers GROUP BY 1 ORDER BY 1"
).fetchall():
    print(f"  {d}: {c:,}")

con.close()
print("\nWarehouse looks healthy.")
