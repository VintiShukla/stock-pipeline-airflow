Stock pipeline demo with Airflow + Postgres.
Files: docker-compose.yml, Dockerfile, requirements.txt, dags/stock_dag.py, .env
Fill .env with ALPHAVANTAGE_API_KEY before running.

This project implements an automated data pipeline that fetches stock market data from a public API, processes the JSON response, and stores the cleaned data into a PostgreSQL database. The entire system is containerized using Docker and orchestrated using Apache Airflow, fulfilling all requirements of the assignment.
The pipeline is built on three core components:

1. API Layer (Alpha Vantage)

Provides stock intraday price data in JSON format.

2. Airflow Orchestrator

Runs two tasks:

prepare_db: creates the database table if it does not exist

fetch_and_store: fetches, parses, cleans, and upserts the JSON data into PostgreSQL

The DAG is scheduled to run hourly (@hourly) and includes retries and logging.

3. PostgreSQL Database

Stores parsed stock data in the stock_prices table using (symbol, timestamp) as a composite primary key. Upsert logic ensures no duplicates.

Data Flow

Airflow triggers the DAG every hour.

The DAG calls the Alpha Vantage API and retrieves JSON for symbols such as IBM and MSFT.

The JSON is parsed and cleaned:

Convert string numbers to float

Convert timestamps to datetime objects

Handle missing or invalid values

The cleaned data is upserted into PostgreSQL.

Logs and status can be viewed in the Airflow web UI.
The entire system runs via docker-compose.yml, which includes:

A PostgreSQL service

An Airflow service (scheduler + webserver)

Mounted volumes for logs, DAGs, and database storage

Environment variables passed from .env for API key and DB credentials

To start the pipeline:

docker compose up --build


Airflow UI is available at:

http://localhost:8080

The pipeline includes:

Retry logic in Airflow

Graceful handling of API rate limits

Safe parsing for numeric and timestamp values

Logging for missing fields or API failures

Upserts to avoid duplicate rows