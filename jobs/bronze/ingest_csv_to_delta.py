#!/usr/bin/env python3
"""
Spark Job: CSV Ingestion to Delta Lake (Bronze Layer)

Reads CSV files from incoming data directory and writes to Delta Lake format
in the Bronze layer with proper schema inference and data validation.
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with Delta Lake support"""
    return SparkSession.builder \
        .appName("Bronze_CSV_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def validate_environment():
    """Validate required environment variables"""
    required_vars = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

def ingest_csv_to_delta(spark, input_path, output_path):
    """Ingest CSV files to Delta Lake format"""
    print(f"Reading CSV files from: {input_path}")
    
    # Read CSV files with header and schema inference
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiline", "true") \
        .option("escape", '"') \
        .csv(input_path)
    
    if df.count() == 0:
        print("No data found in input path. Skipping...")
        return
    
    print(f"Found {df.count()} records")
    print("Schema:")
    df.printSchema()
    
    # Add metadata columns
    df_with_metadata = df \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit("csv")) \
        .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    print(f"Writing to Delta Lake: {output_path}")
    
    # Write to Delta Lake with append mode
    df_with_metadata.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(output_path)
    
    print("Successfully ingested data to Bronze layer")

def main():
    """Main execution function"""
    try:
        # Validate environment
        validate_environment()
        
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Define paths
        input_path = "/data/incoming/claims/*.csv"
        output_path = "s3a://lake/bronze/claims"
        
        print(f"Starting CSV ingestion job at {datetime.now()}")
        print(f"Input path: {input_path}")
        print(f"Output path: {output_path}")
        
        # Perform ingestion
        ingest_csv_to_delta(spark, input_path, output_path)
        
        print("Job completed successfully!")
        
    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()