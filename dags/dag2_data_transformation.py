from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd


# ---------------- DAG CONFIG ----------------
dag = DAG(
    dag_id="data_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["transformation", "postgres"]
)


# ---------------- TASK 1 ----------------
def create_transformed_table():
    """
    Creates transformed_employee_data table if it does not exist.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary FLOAT,
        join_date DATE,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """

    hook.run(create_table_sql)


# ---------------- TASK 2 ----------------
def transform_and_load():
    """
    Reads data from raw_employee_data, applies transformations,
    and loads into transformed_employee_data.

    Returns:
        dict: processing statistics
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Read raw data
    df = pd.read_sql("SELECT * FROM raw_employee_data", engine)

    rows_processed = len(df)

    # ---------------- TRANSFORMATIONS ----------------
    df["full_info"] = df["name"] + " - " + df["city"]

    df["age_group"] = df["age"].apply(
        lambda x: "Young" if x < 30 else ("Mid" if 30 <= x < 50 else "Senior")
    )

    df["salary_category"] = df["salary"].apply(
        lambda x: "Low" if x < 50000 else ("Medium" if 50000 <= x < 80000 else "High")
    )

    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year

    # Ensure idempotency
    hook.run("TRUNCATE TABLE transformed_employee_data;")

    # Load transformed data
    df.to_sql(
        name="transformed_employee_data",
        con=engine,
        if_exists="append",
        index=False,
        method="multi"
    )

    rows_inserted = len(df)

    return {
        "rows_processed": rows_processed,
        "rows_inserted": rows_inserted
    }


# ---------------- OPERATORS ----------------
create_transformed_table_task = PythonOperator(
    task_id="create_transformed_table",
    python_callable=create_transformed_table,
    dag=dag
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_and_load,
    dag=dag
)


# ---------------- DEPENDENCIES ----------------
create_transformed_table_task >> transform_and_load_task
