from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Paths inside the Airflow container
BASE_PATH = "/opt/airflow"
RAW_PATH = os.path.join(BASE_PATH, "data", "raw", "transactions.csv")
CLEAN_PATH = os.path.join(BASE_PATH, "data", "clean", "transactions_clean.csv")
ANALYTICS_PATH = os.path.join(BASE_PATH, "data", "analytics", "fraud_summary.csv")


def ingest_data():
    """
    In a real project this would copy data from an API / DB / bucket.
    For now we just check that the raw file exists.
    """
    if not os.path.exists(RAW_PATH):
        raise FileNotFoundError(f"Raw file not found at {RAW_PATH}")
    print(f"Raw file found at {RAW_PATH}")


def clean_data():
    """
    Basic cleaning: remove nulls, keep only positive amounts.
    """
    os.makedirs(os.path.dirname(CLEAN_PATH), exist_ok=True)

    df = pd.read_csv(RAW_PATH)

    # Drop rows with any missing values
    df = df.dropna()

    # Keep only positive amounts
    df = df[df["amount"] > 0]

    df.to_csv(CLEAN_PATH, index=False)
    print(f"Cleaned data written to {CLEAN_PATH}")


def transform_data():
    """
    Add a simple fraud flag: amount > 50,000 => suspicious.
    """
    df = pd.read_csv(CLEAN_PATH)

    df["is_suspicious"] = df["amount"] > 50000

    df.to_csv(CLEAN_PATH, index=False)
    print("Transform step completed, is_suspicious column added.")


def load_data():
    """
    Create an analytics summary and save it.
    """
    os.makedirs(os.path.dirname(ANALYTICS_PATH), exist_ok=True)

    df = pd.read_csv(CLEAN_PATH)

    summary = (
        df.groupby("is_suspicious")["amount"]
        .agg(transaction_count="count", total_amount="sum")
        .reset_index()
    )

    summary.to_csv(ANALYTICS_PATH, index=False)
    print(f"Analytics summary written to {ANALYTICS_PATH}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="transaction_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="ingest_data", python_callable=ingest_data)
    t2 = PythonOperator(task_id="clean_data", python_callable=clean_data)
    t3 = PythonOperator(task_id="transform_data", python_callable=transform_data)
    t4 = PythonOperator(task_id="load_data", python_callable=load_data)

    t1 >> t2 >> t3 >> t4
