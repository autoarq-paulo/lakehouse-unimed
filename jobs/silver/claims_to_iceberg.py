#!/usr/bin/env python3
"""
Spark Job: Bronze to Silver Transformation (Claims Data)

Reads data from Bronze Delta Lake, applies data quality rules,
and writes cleaned/enriched data to Silver layer in Iceberg format.
"""

import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, trim, upper, regexp_replace,
    current_timestamp, lit, coalesce, to_date, year, month, dayofmonth
)
from pyspark.sql.types import *

def create_spark_session():
    """Create Spark session with Delta Lake and Iceberg support"""
    return SparkSession.builder \
        .appName("Silver_Claims_Transformation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
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

def clean_and_transform_claims(df):
    """Apply data quality and transformation rules to claims data"""
    print("Applying data quality transformations...")
    
    # Data cleaning transformations
    df_cleaned = df \
        .filter(col("claim_id").isNotNull()) \
        .withColumn("claim_id", trim(col("claim_id"))) \
        .withColumn("member_id", trim(col("member_id"))) \
        .withColumn("provider_name", 
                   when(col("provider_name").isNull() | (trim(col("provider_name")) == ""), "UNKNOWN")
                   .otherwise(upper(trim(col("provider_name"))))) \
        .withColumn("claim_amount", 
                   when(col("claim_amount").isNull() | (col("claim_amount") < 0), 0.0)
                   .otherwise(col("claim_amount"))) \
        .withColumn("service_date", 
                   coalesce(to_date(col("service_date"), "yyyy-MM-dd"),
                           to_date(col("service_date"), "MM/dd/yyyy"),
                           to_date(col("service_date"), "dd/MM/yyyy")))
    
    # Add derived columns
    df_enriched = df_cleaned \
        .withColumn("service_year", year(col("service_date"))) \
        .withColumn("service_month", month(col("service_date"))) \
        .withColumn("service_day", dayofmonth(col("service_date"))) \
        .withColumn("claim_amount_category", 
                   when(col("claim_amount") == 0, "ZERO")
                   .when(col("claim_amount") <= 100, "LOW")
                   .when(col("claim_amount") <= 1000, "MEDIUM")
                   .when(col("claim_amount") <= 10000, "HIGH")
                   .otherwise("VERY_HIGH")) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("data_quality_score", 
                   when(col("claim_id").isNull(), 0)
                   .when(col("member_id").isNull(), 0.3)
                   .when(col("service_date").isNull(), 0.5)
                   .when(col("claim_amount").isNull() | (col("claim_amount") <= 0), 0.7)
                   .otherwise(1.0))
    
    # Filter out low quality records
    df_quality = df_enriched.filter(col("data_quality_score") >= 0.5)
    
    print(f"Records after quality filtering: {df_quality.count()}")
    return df_quality

def create_iceberg_table_if_not_exists(spark):
    """Create Iceberg table schema if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.claims (
        claim_id string,
        member_id string,
        provider_name string,
        claim_amount double,
        service_date date,
        service_year int,
        service_month int,
        service_day int,
        claim_amount_category string,
        processing_timestamp timestamp,
        data_quality_score double,
        ingestion_timestamp timestamp,
        source_file string,
        batch_id string
    )
    USING ICEBERG
    PARTITIONED BY (service_year, service_month)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    """
    
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
        spark.sql(create_table_sql)
        print("Iceberg table created/verified successfully")
    except Exception as e:
        print(f"Warning: Could not create Iceberg table: {e}")
        print("Proceeding with write operation...")

def write_to_iceberg_and_delta(df, iceberg_table, delta_path):
    """Write data to both Iceberg table and Delta Lake"""
    print(f"Writing to Iceberg table: {iceberg_table}")
    
    # Write to Iceberg (append mode)
    try:
        df.writeTo(iceberg_table).append()
        print("Successfully wrote to Iceberg table")
    except Exception as e:
        print(f"Warning: Iceberg write failed: {e}")
        print("Falling back to Delta Lake only...")
    
    # Also mirror to Delta Lake for backup/compatibility
    print(f"Writing to Delta Lake: {delta_path}")
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("service_year", "service_month") \
        .save(delta_path)
    
    print("Successfully wrote to Delta Lake")

def main():
    """Main execution function"""
    try:
        # Validate environment
        validate_environment()
        
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Define paths
        bronze_path = "s3a://lake/bronze/claims"
        iceberg_table = "iceberg.silver.claims"
        silver_delta_path = "s3a://lake/silver/claims"
        
        print(f"Starting Silver transformation job at {datetime.now()}")
        print(f"Bronze source: {bronze_path}")
        print(f"Iceberg target: {iceberg_table}")
        print(f"Delta backup: {silver_delta_path}")
        
        # Create Iceberg table if needed
        create_iceberg_table_if_not_exists(spark)
        
        # Read from Bronze Delta Lake
        print("Reading from Bronze layer...")
        df_bronze = spark.read.format("delta").load(bronze_path)
        
        if df_bronze.count() == 0:
            print("No data found in Bronze layer. Exiting...")
            return
        
        print(f"Read {df_bronze.count()} records from Bronze")
        
        # Apply transformations
        df_silver = clean_and_transform_claims(df_bronze)
        
        # Write to Silver layer
        write_to_iceberg_and_delta(df_silver, iceberg_table, silver_delta_path)
        
        print("Silver transformation completed successfully!")
        
    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        sys.exit(1)
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()