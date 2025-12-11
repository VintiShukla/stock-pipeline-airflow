from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta
import requests
import psycopg2
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.decorators import task

DAG_ID = "fetch_store_stock_data"
DEFAULT_ARGS = {
    "owner": "vinti",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SCHEDULE_INTERVAL = "@hourly"

ALPHA_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")
DB_HOST = os.getenv("STOCK_DB_HOST", "postgres")
DB_PORT = int(os.getenv("STOCK_DB_PORT", 5432))
DB_NAME = os.getenv("STOCK_DB_NAME", "stock_db")
DB_USER = os.getenv("STOCK_DB_USER", "stock_user")
DB_PASSWORD = os.getenv("STOCK_DB_PASSWORD", "stock_pass")

TABLE_NAME = "stock_prices"

def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )
    return conn

def ensure_table_exists():
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        symbol TEXT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        open NUMERIC,
        high NUMERIC,
        low NUMERIC,
        close NUMERIC,
        volume BIGINT,
        PRIMARY KEY (symbol, timestamp)
    );
    """
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
    finally:
        conn.close()

def upsert_stock_rows(rows):
    if not rows:
        return
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                insert_sql = f"""
                INSERT INTO {TABLE_NAME} (symbol, timestamp, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (symbol, timestamp)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
                """
                execute_values(cur, insert_sql, rows)
    finally:
        conn.close()

def fetch_alpha_vantage_intraday(symbol="IBM", interval="60min", outputsize="compact"):
    if not ALPHA_KEY:
        raise ValueError("ALPHAVANTAGE_API_KEY is not set in environment")

    base = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "datatype": "json",
        "apikey": ALPHA_KEY,
    }
    resp = requests.get(base, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

default_args = DEFAULT_ARGS

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Fetch stock JSON and store in Postgres",
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "assignment"],
) as dag:

    @task(retries=2, retry_delay=timedelta(minutes=3))
    def prepare_db():
        logging.info("Ensuring stock table exists")
        ensure_table_exists()
        return True

    @task()
    def fetch_and_store(symbols_str: str = "IBM,MSFT"):
        symbols = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        all_rows = []
        problems = []
        for symbol in symbols:
            try:
                logging.info(f"Fetching data for {symbol}")
                data = fetch_alpha_vantage_intraday(symbol=symbol)
                ts_key = None
                for k in data.keys():
                    if k.startswith("Time Series"):
                        ts_key = k
                        break
                if not ts_key:
                    problems.append((symbol, "no time series key", data))
                    logging.warning(f"No time series for {symbol}: {data}")
                    continue

                timeseries = data[ts_key]
                for time_str, point in timeseries.items():
                    try:
                        ts = datetime.fromisoformat(time_str)
                    except Exception:
                        ts = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")

                    def _safe_get(key):
                        val = point.get(key)
                        if val in (None, "", "null"):
                            return None
                        try:
                            return float(val.replace(",", "")) if isinstance(val, str) else float(val)
                        except Exception:
                            return None

                    open_v = _safe_get("1. open") or _safe_get("open")
                    high_v = _safe_get("2. high") or _safe_get("high")
                    low_v = _safe_get("3. low") or _safe_get("low")
                    close_v = _safe_get("4. close") or _safe_get("close")
                    volume_v_raw = point.get("5. volume") or point.get("volume") or None
                    try:
                        volume_v = int(volume_v_raw) if volume_v_raw not in (None, "") else None
                    except Exception:
                        volume_v = None

                    row = (symbol, ts, open_v, high_v, low_v, close_v, volume_v)
                    all_rows.append(row)

            except Exception as e:
                logging.exception(f"Failed to process symbol {symbol}: {e}")
                problems.append((symbol, str(e)))

        if all_rows:
            upsert_stock_rows(all_rows)
            logging.info(f"Upserted {len(all_rows)} rows.")
        if problems:
            logging.warning(f"Problems encountered: {problems}")
        return {"inserted": len(all_rows), "problems": problems}

    prepare_db() >> fetch_and_store("IBM,MSFT")
