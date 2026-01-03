from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime


# ---------------- DAG CONFIG ----------------
dag = DAG(
    dag_id="conditional_workflow_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["conditional", "branching"]
)


# ---------------- BRANCH LOGIC ----------------
def choose_branch(execution_date, **context):
    """
    Decide branch based on day of week.
    Monday = 0, Sunday = 6
    """
    day = execution_date.weekday()

    if day <= 2:          # Mon–Wed
        return "weekday_task_1"
    elif day <= 4:        # Thu–Fri
        return "end_of_week_task_1"
    else:                 # Sat–Sun
        return "weekend_task_1"


# ---------------- TASK FUNCTIONS ----------------
def weekday_task():
    return {"branch": "weekday", "status": "processed"}

def weekday_summary():
    return {"summary": "weekday summary generated"}

def end_of_week_task():
    return {"branch": "end_of_week", "status": "processed"}

def end_of_week_report():
    return {"report": "end-of-week report generated"}

def weekend_task():
    return {"branch": "weekend", "status": "processed"}

def weekend_cleanup():
    return {"cleanup": "weekend cleanup completed"}


# ---------------- OPERATORS ----------------
start = EmptyOperator(
    task_id="start",
    dag=dag
)

branch = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=choose_branch,
    dag=dag
)

# Weekday branch
weekday_task_1 = PythonOperator(
    task_id="weekday_task_1",
    python_callable=weekday_task,
    dag=dag
)

weekday_task_2 = PythonOperator(
    task_id="weekday_task_2",
    python_callable=weekday_summary,
    dag=dag
)

# End-of-week branch
end_of_week_task_1 = PythonOperator(
    task_id="end_of_week_task_1",
    python_callable=end_of_week_task,
    dag=dag
)

end_of_week_task_2 = PythonOperator(
    task_id="end_of_week_task_2",
    python_callable=end_of_week_report,
    dag=dag
)

# Weekend branch
weekend_task_1 = PythonOperator(
    task_id="weekend_task_1",
    python_callable=weekend_task,
    dag=dag
)

weekend_task_2 = PythonOperator(
    task_id="weekend_task_2",
    python_callable=weekend_cleanup,
    dag=dag
)

# End task (runs always)
end = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag
)


# ---------------- DEPENDENCIES ----------------
start >> branch

branch >> weekday_task_1 >> weekday_task_2 >> end
branch >> end_of_week_task_1 >> end_of_week_task_2 >> end
branch >> weekend_task_1 >> weekend_task_2 >> end
