import os
from datetime import datetime, date

import psycopg2
from dotenv import load_dotenv

from generate_orders import main as generate_orders_main
from load_raw import load_raw_data_to_postgres
from transform_staging import build_staging_table
from transform_mart import build_marts

# Load environment variables
load_dotenv()
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "de_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "de_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "de_password")

def run_pipelines():
    today = datetime.today().strftime("%Y%m%d")
    csv_path = f"data/raw/orders_{today}.csv"

    # Step 1: Generate Orders CSV
    generate_orders_main(today)

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

    try:
        start_time = datetime.now()

        # Step 2: Load Raw Data into PostgreSQL
        load_raw_data_to_postgres(csv_path, conn)
        print("Loaded raw data into PostgreSQL.")

        # Step 3: Transform and Load Staging Data
        build_staging_table(conn)
        print("Transformed and loaded staging data.")

        # Step 4: Build Marts
        build_marts(conn)
        print("Built marts.")

    finally:
        conn.close()
        print("Closed PostgreSQL connection.")
        duration = datetime.now() - start_time
        print(f"Pipeline completed in {duration}.")

if __name__ == "__main__":
    run_pipelines()
