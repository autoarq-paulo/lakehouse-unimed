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
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

# DAG definition
dag = DAG(
    'silver_claims_transform',
    default_args=default_args,
    description='Transform Bronze data to Silver layer (Iceberg)',
    schedule_interval=timedelta(hours=2),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'transformation', 'iceberg']
)

# Wait for bronze ingestion to complete
wait_for_bronze = ExternalTaskSensor(
    task_id='wait_for_bronze_ingestion',
    external_dag_id='bronze_ingest_csv',
    external_task_id='ingest_claims_csv_to_delta',
    timeout=300,
    poke_interval=60,
    dag=dag
)

# Transform bronze data to silver (Iceberg)
transform_claims = BashOperator(
    task_id='transform_claims_to_iceberg',
    bash_command='''
    spark-submit \
        --master local[*] \
        --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
        --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg.type=hive \
        --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
        --conf spark.hadoop.fs.s3a.endpoint={{ var.value.S3_ENDPOINT }} \
        --conf spark.hadoop.fs.s3a.access.key={{ var.value.MINIO_ROOT_USER }} \
        --conf spark.hadoop.fs.s3a.secret.key={{ var.value.MINIO_ROOT_PASSWORD }} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --packages io.delta:delta-core_2.12:2.4.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
        /opt/jobs/silver/claims_to_iceberg.py
    ''',
    dag=dag
)

# Task dependencies
wait_for_bronze >> transform_claims