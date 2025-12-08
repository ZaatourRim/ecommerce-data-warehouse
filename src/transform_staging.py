
def build_staging_table(postgres_conn):
    """Transform and load data from raw.orders_all to staging.orders_staging in PostgreSQL database."""

    with postgres_conn.cursor() as cur:
        # 1) DROP existing data in staging table
        cur.execute("DROP TABLE IF EXISTS staging.orders_stg;")
    
        # 2) Recreate transformed data from raw to staging
        cur.execute("""
            CREATE TABLE staging.orders_stg AS
            SELECT
                order_id,
                order_timestamp,
                customer_id,
                product_id,
                category,
                price,
                quantity,
                status,
                price * quantity AS total_amount
            FROM raw.orders_all
            WHERE price > 0
            AND quantity > 0
            AND order_timestamp IS NOT NULL
            AND status IN ('pending', 'shipped', 'delivered', 'cancelled');
            """)
        # 3) Add indexes for performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_stg_order_ts
            ON staging.orders_stg (order_timestamp);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_orders_stg_customer
            ON staging.orders_stg (customer_id);
        """)
            
    postgres_conn.commit()
    print("Transformed and loaded data into staging.orders_stg")

if __name__ == "__main__":
    import psycopg2
    import os
    from dotenv import load_dotenv 

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
    build_staging_table(conn)
    conn.close()
