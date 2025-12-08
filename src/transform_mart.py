import psycopg2
import os
from dotenv import load_dotenv


def build_marts(conn):
    with conn.cursor() as cur:
        # 1) DROP + CREATE mart.daily_revenue
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS mart;
        """)
        cur.execute("""
            DROP TABLE IF EXISTS mart.daily_revenue;
        """)
        cur.execute("""
            CREATE TABLE mart.daily_revenue AS
            SELECT
                DATE_TRUNC('day', order_timestamp) AS order_date,
                SUM(total_amount) AS revenue
            FROM staging.orders_stg
            GROUP BY order_date
            ORDER BY order_date;

        """)
        # 2) DROP + CREATE mart.customer_ltv
        cur.execute("""
            DROP TABLE IF EXISTS mart.customer_ltv;
        """)
        cur.execute("""
            CREATE TABLE mart.customer_ltv AS
            SELECT
                customer_id,
                SUM(total_amount) AS lifetime_value
            FROM staging.orders_stg
            GROUP BY customer_id;

        """)
        # 3) DROP + CREATE mart.product_revenue
        cur.execute("""
            DROP TABLE IF EXISTS mart.product_revenue;
        """)
        cur.execute("""
            CREATE TABLE mart.product_revenue AS
            SELECT
                product_id,
                category,
                SUM(total_amount)       AS total_revenue,
                SUM(quantity)           AS total_quantity,
                COUNT(*)                AS order_count,
                AVG(total_amount)       AS avg_order_value
            FROM staging.orders_stg
            GROUP BY product_id, category;
        """)
        # 4) DROP + CREATE mart.customer_segments
        cur.execute("""
            DROP TABLE IF EXISTS mart.customer_segments;
        """)
        cur.execute("""
            CREATE TABLE mart.customer_segments AS
            SELECT
                customer_id,
                lifetime_value,
                CASE
                    WHEN lifetime_value >= 10000 THEN 'VIP'
                    WHEN lifetime_value >= 3000  THEN 'High'
                    WHEN lifetime_value >= 500   THEN 'Medium'
                    ELSE 'Low'
                END AS segment
            FROM mart.customer_ltv;
        """)

    conn.commit()
    print("Rebuilt mart schema (daily_revenue, customer_ltv, product_revenue, customer_segments)")


if __name__ == "__main__":

    load_dotenv()
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "de_db")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "de_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    build_marts(conn)
    conn.close()
