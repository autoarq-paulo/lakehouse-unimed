from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Default args for DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'gold_publish_views',
    default_args=default_args,
    description='Create Gold layer views in Trino',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'views', 'trino']
)

# Wait for silver transformation to complete
wait_for_silver = ExternalTaskSensor(
    task_id='wait_for_silver_transformation',
    external_dag_id='silver_claims_transform',
    external_task_id='transform_claims_to_iceberg',
    timeout=600,
    poke_interval=120,
    dag=dag
)

# Create Gold views in Trino
create_gold_views = BashOperator(
    task_id='create_gold_views',
    bash_command='''
    trino --server http://trino:8080 \
          --catalog lake \
          --schema gold \
          --execute-file /opt/sql/gold/create_views.sql
    ''',
    dag=dag
)

# Run data quality checks
run_trino_checks = BashOperator(
    task_id='run_trino_checks',
    bash_command='''
    trino --server http://trino:8080 \
          --catalog lake \
          --execute-file /opt/sql/trino_checks.sql
    ''',
    dag=dag
)

# Task dependencies
wait_for_silver >> create_gold_views >> run_trino_checks