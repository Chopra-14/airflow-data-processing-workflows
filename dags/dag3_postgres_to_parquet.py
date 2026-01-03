from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os


# ---------------- DAG CONFIG ----------------
dag = DAG(
    dag_id="postgres_to_parquet_export",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["export", "parquet", "analytics"]
)


# ---------------- TASK 1 ----------------
def check_table_exists_and_has_data():
    """
    Checks whether transformed_employee_data table exists
    and contains at least one row.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")

    check_sql = """
    SELECT COUNT(*) 
    FROM transformed_employee_data;
    """

    records = hook.get_first(check_sql)

    if records is None or records[0] == 0:
        raise ValueError("transformed_employee_data table is empty or does not exist")

    return True


# ---------------- TASK 2 ----------------
def export_to_parquet(ds=None):
    """
    Exports transformed_employee_data table to Parquet format.

    Returns:
        dict: file metadata
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    output_dir = "/opt/airflow/output"
    os.makedirs(output_dir, exist_ok=True)

    file_path = f"{output_dir}/employee_data_{ds}.parquet"

    # Read from Postgres
    df = pd.read_sql("SELECT * FROM transformed_employee_data", engine)

    # Write Parquet using pyarrow + snappy
    df.to_parquet(
        file_path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )

    return {
        "file_path": file_path,
        "row_count": len(df),
        "file_size_bytes": os.path.getsize(file_path)
    }


# ---------------- TASK 3 ----------------
def validate_parquet(ds=None):
    """
    Validates the generated Parquet file.
    """
    file_path = f"/opt/airflow/output/employee_data_{ds}.parquet"

    if not os.path.exists(file_path):
        raise FileNotFoundError("Parquet file not found")

    df = pd.read_parquet(file_path)

    if df.empty:
        raise ValueError("Parquet file is empty")

    required_columns = {
        "id", "name", "age", "city", "salary", "join_date",
        "full_info", "age_group", "salary_category", "year_joined"
    }

    if not required_columns.issubset(set(df.columns)):
        raise ValueError("Parquet schema validation failed")

    return True


# ---------------- OPERATORS ----------------
check_table_task = PythonOperator(
    task_id="check_source_table",
    python_callable=check_table_exists_and_has_data,
    dag=dag
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_to_parquet,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    op_kwargs={"ds": "{{ ds }}"},
    dag=dag
)


# ---------------- DEPENDENCIES ----------------
check_table_task >> export_task >> validate_task
