from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

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
    'bronze_ingest_csv',
    default_args=default_args,
    description='Ingest CSV files to Bronze layer (Delta Lake)',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'ingestion', 'delta']
)

# Task to run Spark job for CSV ingestion
ingest_claims_csv = BashOperator(
    task_id='ingest_claims_csv_to_delta',
    bash_command='''
    spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.hadoop.fs.s3a.endpoint={{ var.value.S3_ENDPOINT }} \
        --conf spark.hadoop.fs.s3a.access.key={{ var.value.MINIO_ROOT_USER }} \
        --conf spark.hadoop.fs.s3a.secret.key={{ var.value.MINIO_ROOT_PASSWORD }} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/jobs/bronze/ingest_csv_to_delta.py
    ''',
    dag=dag
)

# Task dependencies
ingest_claims_csv