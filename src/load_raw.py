import psycopg2
import os
from dotenv import load_dotenv 
from datetime import datetime

load_dotenv()

def load_raw_data_to_postgres(csv_path, postgres_conn):
    """Load raw data from CSV into PostgreSQL database, into the table 'raw.orders_raw'."""

    # 1) Truncate existing data for demo
    with postgres_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw.orders_raw;")
    
    # 2) open CSV and run COPY command
        with open(csv_path, 'r') as f:
            cur.copy_expert(
                sql="""
                    COPY raw.orders_raw 
                    FROM STDIN 
                    WITH CSV HEADER DELIMITER ','
                """,
                file=f
        )
        
        cur.execute("""
            INSERT INTO raw.orders_all(
                order_id,
                order_timestamp,
                customer_id,
                product_id,
                category,
                price,
                quantity,
                status,
                load_date
            )
            SELECT
                order_id,
                order_timestamp,
                customer_id,
                product_id,
                category,
                price,
                quantity,
                status,
                CURRENT_DATE
            FROM raw.orders_raw;
            """)
        
        cur.execute("SELECT COUNT(*) FROM raw.orders_raw;")
        rowcount = cur.fetchone()[0]
    postgres_conn.commit()
    print(f"Loaded {rowcount} rows into raw.orders_raw and appended to raw.orders_all")

if __name__ == "__main__":
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
    today = datetime.today().strftime("%Y%m%d")
    csv_path = f"data/raw/orders_{today}.csv"
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}, Run generate_orders.py first.")
    load_raw_data_to_postgres(csv_path, conn)
    conn.close()