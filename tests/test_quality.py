import os
import psycopg2
from dotenv import load_dotenv
from datetime import date 

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "de_db"),
        user=os.getenv("POSTGRES_USER", "de_user"),
        password=os.getenv("POSTGRES_PASSWORD", "de_password"),
    )

def test_raw_not_empty():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM raw.orders_raw;")
    count = cur.fetchone()[0]
    conn.close()

    assert count > 0, "raw.orders_raw is empty — load_raw step failed"

def test_order_id_not_null():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.orders_raw WHERE order_id IS NULL;")
    null_count = cur.fetchone()[0]
    conn.close()

    assert null_count == 0, "order_id contains NULL values"

def test_staging_not_smaller_than_raw_today():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.orders_raw;")
    raw_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.orders_stg;")
    staging_count = cur.fetchone()[0]

    conn.close()

    assert staging_count > 0, "staging.orders_stg is empty, transform staging step failed"
    assert staging_count >= raw_count, (
        f"staging should have at least today's raw data of {raw_count} rows")
    
def test_raw_all_not_empty_and_superset_of_raw():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM raw.orders_raw;")
    raw_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM raw.orders_all;")
    all_count = cur.fetchone()[0]

    conn.close()

    assert all_count > 0, "raw.orders_all is empty — incremental history not working"
    assert all_count >= raw_count, (
        f"raw.orders_all ({all_count}) should have at least as many rows as raw.orders_raw ({raw_count})"
    )

def test_total_amount_positive():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT MIN(total_amount) FROM staging.orders_stg;")
    min_value = cur.fetchone()[0]

    conn.close()

    assert min_value >= 0, "Found negative total_amount values in staging"

def test_mart_daily_revenue_not_empty():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM mart.daily_revenue;")
    count = cur.fetchone()[0]

    conn.close()

    assert count > 0, "mart.daily_revenue is empty"

def test_mart_customer_ltv_not_empty():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM mart.customer_ltv;")
    count = cur.fetchone()[0]

    conn.close()

    assert count > 0, "mart.customer_ltv is empty"

def test_dates_in_valid_range():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT MIN(order_timestamp), MAX(order_timestamp) FROM staging.orders_stg;")
    min_ts, max_ts = cur.fetchone()

    conn.close()

    assert min_ts.date() >= date(2023, 1, 1), "Dates start earlier than expected"
    assert max_ts.date() <= date(2025, 11, 1), "Dates exceed expected generator range"

def test_mart_product_revenue_not_empty():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM mart.product_revenue;")
    count = cur.fetchone()[0]

    conn.close()

    assert count > 0, "mart.product_revenue is empty"

def test_mart_customer_segments_not_empty():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM mart.customer_segments;")
    count = cur.fetchone()[0]

    conn.close()

    assert count > 0, "mart.customer_segments is empty"

def test_customer_segments_values_valid():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT segment FROM mart.customer_segments;
    """)
    segments = {row[0] for row in cur.fetchall()}
    conn.close()

    allowed_segments = {"VIP", "High", "Medium", "Low"}
    assert segments.issubset(allowed_segments), (
        f"Found invalid customer segments: {segments - allowed_segments}"
    )

def test_daily_revenue_matches_staging_sum():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
                SELECT SUM(total_amount) FROM staging.orders_stg;
            """)
    staging_total = cur.fetchone()[0]
    cur.execute("""
                SELECT SUM(revenue) FROM mart.daily_revenue;
            """)
    mart_total = cur.fetchone()[0]
    conn.close()

    assert abs(staging_total - mart_total) < 0.0001, (
        f"Total revenue mismatch: staging={staging_total}, mart={mart_total}"
    )

def test_quantity_positive_in_staging():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM staging.orders_stg WHERE quantity <= 0;")
    bad = cur.fetchone()[0]

    conn.close()

    assert bad == 0, "Found non-positive quantities in staging.orders_stg"

def test_price_positive_in_staging():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM staging.orders_stg WHERE price <= 0;")
    bad = cur.fetchone()[0]

    conn.close()

    assert bad == 0, "Found non-positive prices in staging.orders_stg"