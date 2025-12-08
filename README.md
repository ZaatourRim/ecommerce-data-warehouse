# Mini Data Warehouse pipeline (Postgres + Python)
An end-to-end production style data pipeline that ingests raw e-commerce orders, loads them into PostgreSQL warehouse, applies data cleaning and transformations, and builds analytics-ready mart tables for reporting and insights.

This project demonstrates essential Data Engineering skills:
- Incremental ingestion
- Data modeling (raw -> staging -> marts)
- SQL transformations
- Python ETL orchestration via *run_pipelines.py*
- Data quality checks in Pytest
- Indexing and performance optimization
- Reproducible local environment

Lightweight and cloud-free but structured like a real analytics pipeline you'd deploy to Redshift/Snowflake/BigQuery + Airflow.


## Architecture overview

````bash
                +----------------------+
                |  generate_orders.py  |
                |  (fake daily orders) |
                +----------+-----------+
                           |
                           v
                +----------------------+
                |   load_raw.py        |
                | COPY CSV → raw layer |
                +----------+-----------+
                           |
                           v
                +-----------------------------+
                | transform_staging.py        |
                | Clean + enrich → staging    |
                +---------------+-------------+
                                |
                                v
                +--------------------------------------------+
                | transform_mart.py                          |
                | Build marts:                               |
                |  - daily_revenue                           |
                |  - customer_ltv                            |
                |  - product_revenue                         |
                |  - customer_segments                       |
                +----------------+----------------------------+
                                 |
                                 v
                     +------------------------+
                     |    Analytics-ready     |
                     |      mart tables       |
                     +------------------------+
`````

## Data Warehouse layers
1. **Raw layer (raw.*)**
Exact copy of incoming CSV data, no transformations
- Tables : 
    - **orders_raw**: last CSV batch
    - **orders_all**: cumulative historical data (old orders + latest orders batch)

2. **Staging layer (staging.*)**

Standardized, cleaned and validated data:
- Removes invalid price/quantity
- Removes unknown statuses
- Ensures timestamp validite
- Adds computed fields such as **total_amount**
- Adds indexes:

Indexes:
```bash
CREATE INDEX idx_orders_stg_order_ts ON staging.orders_stg(order_timestamp);
CREATE INDEX idx_orders_stg_customer ON staging.orders_stg(customer_id);
```

3. **Mart layer (mart.*)**

Analytics-ready aggregated tables:
| Table | Description | 
|-----------|-----------|
| *daily_revenue* | Revenue per day| 
|*customer_ltv*|Lifetime value per customer |
|*product_revenue*| Revenue, quantity and average order value per product|
|*customer_segments*|LTV (Lifetime value) based customer segmentation (**VIP, High, Medium, Low**)|

## Project Structure and Tech stack

```bash
├── data/
│   └── raw/
│       └── orders_YYYYMMDD.csv
├── src/
│   ├── generate_orders.py
│   ├── load_raw.py
│   ├── transform_staging.py
│   ├── transform_mart.py
│   └── run_pipeline.py
├── tests/
│   └── test_quality.py
├── README.md
└── requirements.txt
````

## Tech stack

- *Python 3.9+*: For ingestion, orchestration, and tests
- *PostgreSQL*: Acts as the warehouse
- *psycopg2*: Database driver
- *dotenv*: Environment variables management
- *Pytest*: Data quality testing
- *CSV*: Source data format


## Setup & Installation
1. Install dependencies
```bash
pip install -r requirements.txt
````
2. Start PostgreSQL (Locally or in Docker)

Create database and user matching your .env config, example:
```bash
POSTGRES_HOST=localhost
POSTGRES_DB=de_db
POSTGRES_USER=de_user
POSTGRES_PASSWORD=de_password
````
3. Run the pipeline
```bash
python src/run_pipeline.py
````
4. Run tests
```bash
pytest -v
````

## Pipeline steps 
### 1. Generate daily orders (Python)
Creates a synthetic dataset with:
- Random customers and products
- Random timestamps (2023 - 2025)
- Weighted order statuses
- Category based price ranges

CSV saved to:
```bash
data/raw/orders_YYYYMMDD.csv
````

### 2. Loads raw CSV into Postgres (raw.orders_raw & raw.orders_all)
*load_raw.py* performs:
- *COPY* ingestion into **raw.orders_raw**
- Append into historical table **raw.orders_all**
- Adds **load_date** column to track loading date ot data

Simulates a real warehouse ingestion layer..

### 3. Build staging table (staging.orders_stg)
*transform_staging.py*:
- Drops and rebuilds the staging table
- Cleans invalid records
- Ensures consistent types
- Adds indexes on:
    - order_timestamp
    - customer_id

### 4. Build marts (mart.*)
*transform_marts.py* produces:

- ***mart.daily_revenue***: Daily total revenue
- ***mart.customer_ltv***: Customer lifetime value
- ***mart.product_revenue***: Product-level metrics
- ***mart.customer_segments***: 
LTV-based customer segmentation

## Data Quality tests
*tests/test_quality.py* validates:

- Raw layer is not empty
- Staging row count >= today’s raw batch
- total_amount is non-negative
- Dates fall within the expected range (2023–2025)
- Marts are populated
- LTV segments valid (VIP/High/Medium/Low)

## Example Queries

Daily revenue:
```bash
SELECT * FROM mart.daily_revenue ORDER BY order_date;
````
Top 10 customers:
```bash
SELECT * FROM mart.customer_ltv ORDER BY lifetime_value DESC LIMIT 10;
````
Product performance:
```bash
SELECT * FROM mart.product_revenue ORDER BY total_revenue DESC;
````
Customer segments:
```bash
SELECT segment, COUNT(*) 
FROM mart.customer_segments 
GROUP BY segment;
```

## Future Improvements

Turn this into a full cloud-based production pipeline:

- Cloud migration
    - Replace Postgres -> Redshift
    - Upload raw CSV -> S3
    - Add AWS Glue catalog for schema management
- Orchestration
    - Replace run_pipeline.py with Airflow DAGs
    - Add retries and alerts
- Scale computing
    - Add Spark for large transformations (raw -> staging -> marts) 
    - Partition staging/marts by date
- Data givernance
    - Add monitoring (Great Expectations)



