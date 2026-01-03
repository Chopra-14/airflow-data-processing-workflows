from airflow.models import DagBag


def test_dag1_loaded():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    assert "csv_to_postgres_ingestion" in dagbag.dags
    assert len(dagbag.import_errors) == 0


def test_dag1_task_count():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    assert len(dag.tasks) == 3


def test_dag1_dependencies():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")

    create_task = dag.get_task("create_table_if_not_exists")
    truncate_task = dag.get_task("truncate_table")
    load_task = dag.get_task("load_csv_to_postgres")

    assert truncate_task in create_task.downstream_list
    assert load_task in truncate_task.downstream_list


from airflow.utils.dag_cycle_tester import check_cycle

def test_dag1_no_cycles():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    check_cycle(dag)


def test_dag1_schedule():
    dagbag = DagBag(dag_folder="dags", include_examples=False)
    dag = dagbag.get_dag("csv_to_postgres_ingestion")
    assert dag.schedule_interval == "@daily"
