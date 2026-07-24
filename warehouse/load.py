"""
warehouse/load.py
-----------------
Builds the DuckDB warehouse (grocery.duckdb) for the Grocery Assistant project.

It lands RAW source data from the scrapers, the receipt bot, and the translation
memory into typed raw_* tables. Historical dated CSVs in stores/ are backfilled so
the warehouse starts with a real price time series rather than a single snapshot.

Design:
  - Idempotent: re-running rebuilds raw tables from whatever CSVs are on disk.
  - RAW layer only. Cleaning / modelling happens downstream in dbt (staging -> marts).
  - Source of truth for the *file* data is the CSVs; source of truth for purchases
    is the Google Sheet 'Raw' tab (loaded via load_receipts()).

Run:
    pip install duckdb pandas gspread
    python warehouse/load.py                 # loads CSVs (AH, Lidl, translation memory)
    python warehouse/load.py --with-receipts  # also pull the Sheet 'Raw' tab (needs creds)

Env / creds (only for --with-receipts):
    grocery_tracker.json  - Google service account key (gitignored)
    SHEET_ID              - from .env
"""

from __future__ import annotations

import argparse
import glob
import os
import re
import sys
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
STORES_DIR = REPO_ROOT / "stores"
DB_PATH = REPO_ROOT / "grocery.duckdb"

DATE_RE = re.compile(r"(\d{8})")


def _date_from_name(path: Path) -> str | None:
    """Extract YYYY-MM-DD from a filename like ah_full_export1_20260724.csv."""
    m = DATE_RE.search(path.name)
    if not m:
        return None
    d = m.group(1)
    return f"{d[0:4]}-{d[4:6]}-{d[6:8]}"


def _collect(patterns: list[str]) -> list[Path]:
    """All non-translated CSVs matching any pattern, root dir + stores/, de-duped."""
    found: list[Path] = []
    for pat in patterns:
        found += [Path(p) for p in glob.glob(str(REPO_ROOT / pat))]
        found += [Path(p) for p in glob.glob(str(STORES_DIR / pat))]
    # drop the *_translated.csv variants — dbt handles translation joins
    found = [p for p in found if "_translated" not in p.name]
    # de-dupe by resolved path
    uniq = sorted({p.resolve() for p in found})
    return list(uniq)


# ---------------------------------------------------------------------------
# Loaders — each reads matching CSVs into one dataframe with a source_date column
# ---------------------------------------------------------------------------
def _load_dated_csvs(patterns: list[str], label: str) -> pd.DataFrame:
    files = _collect(patterns)
    if not files:
        print(f"  [{label}] no files found for {patterns}")
        return pd.DataFrame()
    frames = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str)  # read as str; dbt casts types
        except Exception as e:  # noqa: BLE001
            print(f"  [{label}] SKIP {f.name}: {e}")
            continue
        df["source_date"] = _date_from_name(f)
        df["source_file"] = f.name
        frames.append(df)
    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"  [{label}] {len(files)} files -> {len(out):,} rows")
    return out


def load_ah_products() -> pd.DataFrame:
    return _load_dated_csvs(["ah_full_export1_*.csv"], "ah_products")


def load_ah_summary() -> pd.DataFrame:
    return _load_dated_csvs(["ah_summary1_*.csv"], "ah_summary")


def load_lidl_offers() -> pd.DataFrame:
    return _load_dated_csvs(["lidl_offers_*.csv"], "lidl_offers")


def load_translation_memory() -> pd.DataFrame:
    path = REPO_ROOT / "product_translation_memory.csv"
    if not path.exists():
        print("  [translation_memory] not found")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    print(f"  [translation_memory] {len(df):,} rows")
    return df


def load_receipts_from_sheet() -> pd.DataFrame:
    """Pull the Google Sheet 'Raw' tab (actual purchase history)."""
    try:
        import gspread  # noqa: PLC0415
    except ImportError:
        print("  [receipts] gspread not installed; skipping")
        return pd.DataFrame()
    key_file = REPO_ROOT / "grocery_tracker.json"
    sheet_id = os.getenv("SHEET_ID")
    if not key_file.exists() or not sheet_id:
        print("  [receipts] missing grocery_tracker.json or SHEET_ID; skipping")
        return pd.DataFrame()
    gc = gspread.service_account(filename=str(key_file))
    ws = gc.open_by_key(sheet_id).worksheet("Raw")
    records = ws.get_all_records()
    df = pd.DataFrame(records).astype(str)
    print(f"  [receipts] {len(df):,} rows from Sheet")
    return df


# ---------------------------------------------------------------------------
# Warehouse build
# ---------------------------------------------------------------------------
RAW_TABLES = {
    "raw_ah_products": load_ah_products,
    "raw_ah_summary": load_ah_summary,
    "raw_lidl_offers": load_lidl_offers,
    "raw_translation_memory": load_translation_memory,
}


def build(with_receipts: bool = False) -> None:
    print(f"Building warehouse at {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    tables = dict(RAW_TABLES)
    if with_receipts:
        tables["raw_receipts"] = load_receipts_from_sheet

    for table, loader in tables.items():
        print(f"- {table}")
        df = loader()  # noqa: F841  (referenced by DuckDB below)
        fq = f"raw.{table}"
        if df.empty:
            print(f"  (empty; skipping {fq})")
            continue
        con.execute(f"CREATE OR REPLACE TABLE {fq} AS SELECT * FROM df")
        n = con.execute(f"SELECT count(*) FROM {fq}").fetchone()[0]
        print(f"  wrote {fq}: {n:,} rows")

    print("\nSummary:")
    rows = con.execute(
        """
        SELECT table_name, estimated_size AS approx_rows
        FROM duckdb_tables() WHERE schema_name = 'raw' ORDER BY table_name
        """
    ).fetchall()
    for name, approx in rows:
        print(f"  raw.{name}: ~{approx:,} rows")
    con.close()
    print(f"\nDone. Open with:  duckdb {DB_PATH}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the grocery DuckDB warehouse.")
    ap.add_argument(
        "--with-receipts",
        action="store_true",
        help="Also pull purchase history from the Google Sheet 'Raw' tab.",
    )
    args = ap.parse_args()
    if not STORES_DIR.exists():
        print(f"WARN: {STORES_DIR} not found; only root CSVs will load.", file=sys.stderr)
    build(with_receipts=args.with_receipts)


if __name__ == "__main__":
    main()
