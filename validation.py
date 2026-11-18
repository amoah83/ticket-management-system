"""Validation and incremental loading into the DuckDB warehouse.

Covers FR-1.02 (validation + quarantine with reason code), AC-1.1 (re-running a month
adds no rows), AC-1.2 (rows in = loaded + quarantined) and NFR-07 (run log with counts).

Reason-code priority (first matching rule wins, so each bad row gets exactly one code):
    1. NULL_REQUIRED_FIELD
    2. DROPOFF_NOT_AFTER_PICKUP
    3. NEGATIVE_FARE
    4. UNKNOWN_PICKUP_ZONE
    5. UNKNOWN_DROPOFF_ZONE

Column names follow the TLC yellow-taxi data dictionary. Confirm them against it.
"""
import re

import duckdb

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

REASON_SQL = """
CASE
  WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
       OR fare_amount IS NULL OR PULocationID IS NULL OR DOLocationID IS NULL
    THEN 'NULL_REQUIRED_FIELD'
  WHEN tpep_dropoff_datetime <= tpep_pickup_datetime THEN 'DROPOFF_NOT_AFTER_PICKUP'
  WHEN fare_amount < 0 THEN 'NEGATIVE_FARE'
  WHEN PULocationID NOT IN (SELECT zone_id FROM zones) THEN 'UNKNOWN_PICKUP_ZONE'
  WHEN DOLocationID NOT IN (SELECT zone_id FROM zones) THEN 'UNKNOWN_DROPOFF_ZONE'
  ELSE NULL
END
"""


def _quote(text) -> str:
    return str(text).replace("'", "''")


def load_zone_ids(con: duckdb.DuckDBPyConnection, zone_ids) -> None:
    """Replace the zones table with the given zone IDs."""
    con.execute("CREATE OR REPLACE TABLE zones (zone_id INTEGER)")
    for zone_id in sorted(set(zone_ids)):
        con.execute("INSERT INTO zones VALUES (?)", [int(zone_id)])


def load_zones_from_csv(con: duckdb.DuckDBPyConnection, csv_path, id_column: str = "LocationID") -> None:
    """Load the taxi zone lookup CSV. Check the id column name against the real file."""
    con.execute(
        f"CREATE OR REPLACE TABLE zones AS "
        f"SELECT CAST({id_column} AS INTEGER) AS zone_id "
        f"FROM read_csv_auto('{_quote(csv_path)}')"
    )


def _ensure_tables(con: duckdb.DuckDBPyConnection, source: str) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS zones (zone_id INTEGER)")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS trips AS "
        f"SELECT *, CAST(NULL AS VARCHAR) AS source_month "
        f"FROM read_parquet('{source}') LIMIT 0"
    )
    con.execute(
        f"CREATE TABLE IF NOT EXISTS quarantine AS "
        f"SELECT *, CAST(NULL AS VARCHAR) AS source_month, CAST(NULL AS VARCHAR) AS reason_code "
        f"FROM read_parquet('{source}') LIMIT 0"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS run_log ("
        "source_month VARCHAR, rows_in BIGINT, rows_loaded BIGINT, "
        "rows_quarantined BIGINT, loaded_at TIMESTAMP DEFAULT current_timestamp)"
    )


def load_month(con: duckdb.DuckDBPyConnection, source, month: str) -> dict:
    """Validate one month and load it. Safe to run again: the month is replaced, not added twice."""
    if not MONTH_PATTERN.match(month):
        raise ValueError(f"month must look like 2024-01, got {month!r}")
    src = _quote(source)
    _ensure_tables(con, src)
    con.execute("BEGIN")
    try:
        con.execute(f"DELETE FROM trips WHERE source_month = '{month}'")
        con.execute(f"DELETE FROM quarantine WHERE source_month = '{month}'")
        con.execute(f"DELETE FROM run_log WHERE source_month = '{month}'")
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE staged AS "
            f"SELECT *, {REASON_SQL} AS reason_code FROM read_parquet('{src}')"
        )
        rows_in = con.execute("SELECT COUNT(*) FROM staged").fetchone()[0]
        con.execute(
            f"INSERT INTO trips BY NAME "
            f"SELECT * EXCLUDE (reason_code), CAST('{month}' AS VARCHAR) AS source_month "
            f"FROM staged WHERE reason_code IS NULL"
        )
        con.execute(
            f"INSERT INTO quarantine BY NAME "
            f"SELECT *, CAST('{month}' AS VARCHAR) AS source_month "
            f"FROM staged WHERE reason_code IS NOT NULL"
        )
        loaded = con.execute(f"SELECT COUNT(*) FROM trips WHERE source_month = '{month}'").fetchone()[0]
        quarantined = con.execute(
            f"SELECT COUNT(*) FROM quarantine WHERE source_month = '{month}'"
        ).fetchone()[0]
        if rows_in != loaded + quarantined:
            raise RuntimeError(f"Row mismatch for {month}: in={rows_in}, loaded={loaded}, quarantined={quarantined}")
        con.execute(
            f"INSERT INTO run_log (source_month, rows_in, rows_loaded, rows_quarantined) "
            f"VALUES ('{month}', {rows_in}, {loaded}, {quarantined})"
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return {"month": month, "rows_in": rows_in, "rows_loaded": loaded, "rows_quarantined": quarantined}
