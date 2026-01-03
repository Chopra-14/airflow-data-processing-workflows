# 🚀 Build Data Processing Workflows with Apache Airflow and Docker

## 📌 Project Overview
This project demonstrates a complete, production-style **data engineering workflow** using **Apache Airflow**, **Docker**, and **PostgreSQL**.  
It showcases how to orchestrate ETL pipelines, apply data transformations, export analytics-ready data, implement conditional logic, handle failures, and validate workflows using unit tests.

The project consists of **five distinct DAGs**, each highlighting a different workflow orchestration pattern commonly used in real-world data engineering systems.

---

## 🏗️ Architecture Overview

**Tech Stack:**
- Apache Airflow 2.8
- Docker & Docker Compose
- PostgreSQL (metadata store + warehouse)
- Pandas & PyArrow
- Pytest (unit testing)

**Architecture Flow:**

CSV → PostgreSQL → Transformed PostgreSQL → Parquet  
                     ↘ Conditional Logic  
                     ↘ Notifications & Error Handling  

All services run inside Docker containers for reproducibility and isolation.

---

## 📂 Project Structure

airflow-data-pipeline/
├── dags/
│ ├── dag1_csv_to_postgres.py
│ ├── dag2_data_transformation.py
│ ├── dag3_postgres_to_parquet.py
│ ├── dag4_conditional_workflow.py
│ └── dag5_notification_workflow.py
├── tests/
│ ├── test_dag1.py
│ ├── test_dag2.py
│ └── test_utils.py
├── data/
│ └── input.csv
├── output/
│ └── employee_data_YYYY-MM-DD.parquet
├── logs/
├── plugins/
├── docker-compose.yml
├── requirements.txt
└── README.md

yaml
Copy code

---

## ⚙️ Prerequisites

Make sure the following are installed:
- Docker
- Docker Compose
- Git

---

## 🐳 Setup Instructions (Docker)

### 1️⃣ Clone Repository
```bash
git clone https://github.com/Chopra-14/airflow-data-processing-workflows.git
cd airflow-data-processing-workflows
2️⃣ Start Airflow Environment
bash
Copy code
docker compose up -d
🌐 Access Airflow UI
URL: http://localhost:8080

Username: admin

Password: admin

🔄 DAG Descriptions & Execution
🟢 DAG 1 — CSV to PostgreSQL Ingestion
DAG ID: csv_to_postgres_ingestion

Creates raw_employee_data table

Truncates table (idempotent)

Loads CSV data from data/input.csv

Trigger:
Enable → Trigger DAG
Expected Output:

Table populated with 100 rows

🟢 DAG 2 — Data Transformation Pipeline
DAG ID: data_transformation_pipeline

Transformations:

full_info = name + city

age_group = Young / Mid / Senior

salary_category = Low / Medium / High

year_joined extracted from join_date

Expected Output:

transformed_employee_data table with transformed columns

🟢 DAG 3 — PostgreSQL to Parquet Export
DAG ID: postgres_to_parquet_export

Checks source table

Exports data to Parquet using pyarrow + snappy

Validates file schema

Expected Output:

Parquet file created in output/ directory
Example:

Copy code
employee_data_2026-01-03.parquet
🟢 DAG 4 — Conditional Workflow
DAG ID: conditional_workflow_pipeline

Branching Logic:

Day	Branch
Mon–Wed	Weekday
Thu–Fri	End-of-week
Sat–Sun	Weekend

Uses BranchPythonOperator

Only one branch runs per execution

End task always runs

🟢 DAG 5 — Notifications & Error Handling
DAG ID: notification_workflow

Logic:

Task fails if execution day % 5 == 0

Success & failure callbacks

Cleanup task runs always

Expected Behavior:

Success days → success notification

Failure days → failure notification

Cleanup always executes

🧪 Running Unit Tests
Unit tests validate DAG structure without running Airflow or connecting to databases.

Run tests inside Docker container:
bash
Copy code
docker exec -it airflow_webserver bash
pip install pytest
pytest /opt/airflow/tests -v
Expected Result:
Copy code
12 passed, 0 failed
🛠️ Troubleshooting
❌ DAG not visible?
Ensure file is inside dags/

Check Browse → DAG Import Errors

Restart Airflow:

bash
Copy code
docker compose restart airflow-scheduler airflow-webserver
❌ PostgreSQL connection error?
Verify Airflow connection:

Connection ID: postgres_default

Host: postgres

Schema: airflow_db

User: airflow_user

Password: airflow_pass

❌ Pytest not found?
Install inside container:

bash
Copy code
pip install pytest
✅ Conclusion
This project demonstrates:

End-to-end ETL orchestration

Data transformation best practices

Analytics-ready data export

Conditional workflows

Error handling & notifications

Professional unit testing

It reflects real-world data engineering standards using Apache Airflow.

