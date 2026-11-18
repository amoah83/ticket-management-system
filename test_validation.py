"""Tests for src/validation.py. They import the real module, so CI tests the real code."""
import os

import duckdb
import pytest

from validation import load_month, load_zone_ids

COLUMNS = "tpep_pickup_datetime, tpep_dropoff_datetime, fare_amount, PULocationID, DOLocationID"

SAMPLE_ROWS = """
  (TIMESTAMP '2024-01-01 08:00:00', TIMESTAMP '2024-01-01 08:20:00', 15.0::DOUBLE, 1, 2),
  (TIMESTAMP '2024-01-01 09:00:00', TIMESTAMP '2024-01-01 09:15:00', 8.5::DOUBLE, 2, 3),
  (TIMESTAMP '2024-01-01 09:00:00', TIMESTAMP '2024-01-01 08:50:00', 10.0::DOUBLE, 1, 2),
  (TIMESTAMP '2024-01-01 10:00:00', TIMESTAMP '2024-01-01 10:30:00', -5.0::DOUBLE, 1, 2),
  (TIMESTAMP '2024-01-01 11:00:00', TIMESTAMP '2024-01-01 10:40:00', -3.0::DOUBLE, 1, 2),
  (TIMESTAMP '2024-01-01 12:00:00', TIMESTAMP '2024-01-01 12:10:00', 9.0::DOUBLE, 999, 2),
  (TIMESTAMP '2024-01-01 13:00:00', TIMESTAMP '2024-01-01 13:10:00', 9.0::DOUBLE, 1, 888),
  (TIMESTAMP '2024-01-01 14:00:00', TIMESTAMP '2024-01-01 14:10:00', NULL, 1, 2)
"""
# 8 rows: 2 clean, 1 dropoff, 1 negative fare, 1 both (counted as dropoff),
# 1 unknown pickup zone, 1 unknown dropoff zone, 1 null fare.


def write_parquet(path, rows=SAMPLE_ROWS):
    con = duckdb.connect(":memory:")
    con.execute(
        f"COPY (SELECT * FROM (VALUES {rows}) AS t({COLUMNS})) "
        f"TO '{path}' (FORMAT PARQUET)"
    )
    con.close()


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    load_zone_ids(connection, [1, 2, 3])
    yield connection
    connection.close()


@pytest.fixture
def jan_file(tmp_path):
    path = tmp_path / "yellow_tripdata_2024-01.parquet"
    write_parquet(path)
    return path


def count(con, table, where="1=1"):
    return con.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]


def test_ac_1_2_rows_in_equals_loaded_plus_quarantined(con, jan_file):
    result = load_month(con, jan_file, "2024-01")
    assert result["rows_in"] == 8
    assert result["rows_loaded"] == 2
    assert result["rows_quarantined"] == 6
    assert result["rows_in"] == result["rows_loaded"] + result["rows_quarantined"]
    assert count(con, "trips") == 2
    assert count(con, "quarantine") == 6


def test_fr_1_02_reason_codes(con, jan_file):
    load_month(con, jan_file, "2024-01")
    rows = con.execute(
        "SELECT reason_code, COUNT(*) FROM quarantine GROUP BY reason_code"
    ).fetchall()
    assert dict(rows) == {
        "DROPOFF_NOT_AFTER_PICKUP": 2,
        "NEGATIVE_FARE": 1,
        "UNKNOWN_PICKUP_ZONE": 1,
        "UNKNOWN_DROPOFF_ZONE": 1,
        "NULL_REQUIRED_FIELD": 1,
    }


def test_every_quarantined_row_has_a_reason_code(con, jan_file):
    load_month(con, jan_file, "2024-01")
    assert count(con, "quarantine", "reason_code IS NULL") == 0


def test_clean_table_has_no_violations(con, jan_file):
    load_month(con, jan_file, "2024-01")
    assert count(con, "trips", "fare_amount < 0") == 0
    assert count(con, "trips", "tpep_dropoff_datetime <= tpep_pickup_datetime") == 0
    assert count(
        con, "trips", "PULocationID NOT IN (SELECT zone_id FROM zones) "
        "OR DOLocationID NOT IN (SELECT zone_id FROM zones)"
    ) == 0


def test_ac_1_1_running_twice_adds_no_rows(con, jan_file):
    load_month(con, jan_file, "2024-01")
    load_month(con, jan_file, "2024-01")
    assert count(con, "trips") == 2
    assert count(con, "quarantine") == 6
    assert count(con, "run_log", "source_month = '2024-01'") == 1


def test_ac_1_1_new_month_adds_rows(con, jan_file, tmp_path):
    feb_file = tmp_path / "yellow_tripdata_2024-02.parquet"
    write_parquet(
        feb_file,
        "(TIMESTAMP '2024-02-01 08:00:00', TIMESTAMP '2024-02-01 08:20:00', 12.0::DOUBLE, 1, 2)",
    )
    load_month(con, jan_file, "2024-01")
    load_month(con, feb_file, "2024-02")
    assert count(con, "trips") == 3
    assert count(con, "trips", "source_month = '2024-02'") == 1
    assert count(con, "run_log") == 2


def test_nfr_07_run_log_records_counts(con, jan_file):
    load_month(con, jan_file, "2024-01")
    row = con.execute(
        "SELECT rows_in, rows_loaded, rows_quarantined FROM run_log WHERE source_month = '2024-01'"
    ).fetchone()
    assert row == (8, 2, 6)


def test_warehouse_tables_exist_and_are_populated(con, jan_file):
    load_month(con, jan_file, "2024-01")
    tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
    assert {"trips", "zones", "quarantine", "run_log"} <= tables
    assert count(con, "trips") > 0
    assert count(con, "zones") == 3
    # calendar and hourly_demand are not built yet, so they are not tested here.


def test_invalid_month_label_is_rejected(con, jan_file):
    with pytest.raises(ValueError):
        load_month(con, jan_file, "January 2024")


@pytest.mark.skipif(
    not os.environ.get("JAN2024_PARQUET"),
    reason="set JAN2024_PARQUET to the real yellow_tripdata_2024-01.parquet to run",
)
def test_real_jan2024_baseline():
    """Exact baseline from the ETL tester's own checks on the raw file."""
    source = os.environ["JAN2024_PARQUET"].replace("\\", "/").replace("'", "''")
    connection = duckdb.connect(":memory:")
    ids = connection.execute(
        f"SELECT DISTINCT PULocationID FROM read_parquet('{source}') "
        f"UNION SELECT DISTINCT DOLocationID FROM read_parquet('{source}')"
    ).fetchall()
    # Zones come from the file itself here, so the unknown-zone rule never fires.
    load_zone_ids(connection, [r[0] for r in ids if r[0] is not None])
    result = load_month(connection, source, "2024-01")
    assert result["rows_in"] == 2_964_624
    assert result["rows_loaded"] == 2_926_310
    assert result["rows_quarantined"] == 38_314
    by_reason = dict(
        connection.execute("SELECT reason_code, COUNT(*) FROM quarantine GROUP BY reason_code").fetchall()
    )
    assert by_reason["DROPOFF_NOT_AFTER_PICKUP"] == 870
    assert by_reason["NEGATIVE_FARE"] == 37_444
