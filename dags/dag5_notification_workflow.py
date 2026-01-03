from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime


# ---------------- DAG CONFIG ----------------
dag = DAG(
    dag_id="notification_workflow",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["notifications", "error-handling"]
)


# ---------------- CALLBACKS ----------------
def success_callback(context):
    return {
        "notification_type": "success",
        "status": "sent",
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"])
    }


def failure_callback(context):
    return {
        "notification_type": "failure",
        "status": "sent",
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"]),
        "error": str(context.get("exception"))
    }


# ---------------- TASK FUNCTIONS ----------------
def risky_operation(execution_date, **context):
    """
    Fails if day of month is divisible by 5
    """
    day = execution_date.day

    if day % 5 == 0:
        raise ValueError(f"Simulated failure on day {day}")

    return {
        "status": "success",
        "execution_date": str(execution_date),
        "success": True
    }


def cleanup():
    return {
        "cleanup_status": "completed",
        "timestamp": str(datetime.utcnow())
    }


# ---------------- OPERATORS ----------------
start = EmptyOperator(
    task_id="start",
    dag=dag
)

risky_task = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
    dag=dag
)

success_notification = EmptyOperator(
    task_id="success_notification",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

failure_notification = EmptyOperator(
    task_id="failure_notification",
    trigger_rule=TriggerRule.ALL_FAILED,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)


# ---------------- DEPENDENCIES ----------------
start >> risky_task
risky_task >> [success_notification, failure_notification]
[success_notification, failure_notification] >> cleanup_task
