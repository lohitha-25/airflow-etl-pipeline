from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello(**kwargs):
    out_path = "/opt/airflow/airflow_data/hello.txt"
    now = datetime.utcnow().isoformat()
    with open(out_path, "a") as f:
        f.write(f"Hello from Airflow at {now}\n")
    print("Wrote hello to", out_path)

with DAG(
    dag_id="hello_write_file",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo"],
) as dag:

    t1 = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
        provide_context=True,
    )
