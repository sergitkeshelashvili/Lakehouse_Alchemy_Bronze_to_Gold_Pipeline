# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Layer Ingestion
# MAGIC
# MAGIC
# MAGIC **The bronze_ingestion.py implements the bronze layer ingestion process for a data lakehouse using PySpark. It creates a catalog, schema, and volume in the pyspark_dwh database, then processes JSON data for entities like "customers," "products," and "orders." For each entity, it:
# MAGIC
# MAGIC - Infers the schema from a sample batch of JSON files.
# MAGIC - Reads streaming JSON data from a source path.
# MAGIC - Wraps the data into a payload struct, adds metadata (file path and ingestion timestamp).
# MAGIC - Writes the data to Delta tables in the bronze schema (pyspark_dwh.bronze.<entity>) using a streaming append mode with checkpoints.
# MAGIC
# MAGIC This script sets up the initial ingestion layer, capturing raw data with minimal transformation for further processing in a data lakehouse architecture.**

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, struct
from pyspark.sql.types import StructType

# ------------------------------------------------------
# Setup: Create catalog, schema, and volume
# ------------------------------------------------------
spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

spark.sql("CREATE CATALOG IF NOT EXISTS pyspark_dwh")
spark.sql("CREATE SCHEMA IF NOT EXISTS pyspark_dwh.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS pyspark_dwh.bronze.checkpoint")


# ------------------------------------------------------
# Utility: Infer schema dynamically from sample batch
# ------------------------------------------------------
def infer_schema_from_batch(path: str, spark) -> StructType:
    """
    Read a sample batch of JSON files to infer schema.
    Raise helpful error if path is empty.
    """
    sample = spark.read.format("json").option("multiLine", True).load(path)
    if len(sample.columns) == 0 and len(sample.schema.fields) == 0:
        raise ValueError(f"No files / no schema found at {path}")
    return sample.schema


# ------------------------------------------------------
# Bronze Ingestion Function
# ------------------------------------------------------
def run_bronze_ingestion(spark):
    entities = ["customers", "products", "orders"]

    base_raw_path = "/Volumes/pyspark_dwh/source/source_data"
    bronze_checkpoint_base = "/Volumes/pyspark_dwh/bronze/checkpoint"
    bronze_db = "pyspark_dwh.bronze"   # ✅ Correct schema reference

    for entity in entities:
        print(f"🚀 Starting Bronze ingestion for entity: {entity}")

        # Source folder for this entity
        src_path = f"{base_raw_path}/{entity}/"

        # Infer schema
        schema_entity = infer_schema_from_batch(src_path, spark)

        # Streaming read 
        df_stream = (
            spark.readStream.format("json")
            .option("multiLine", True)
            .schema(schema_entity)
            .load(src_path)
        )

        # Wrap raw JSON into 'payload' + add metadata
        payload_cols = [c for c in df_stream.columns]
        df_bronze = (
            df_stream.select(struct(*payload_cols).alias("payload"), col("_metadata").alias("metadata"))
            .withColumn("ingest_file", col("metadata.file_path"))
            .withColumn("bronze_ingest_ts", current_timestamp())
            .drop("metadata")
        )

        # Write to Delta Bronze table
        checkpoint = f"{bronze_checkpoint_base}/{entity}"
        target_table = f"{bronze_db}.{entity}"   # ✅ pyspark_dwh.bronze.customers, etc.

        (
            df_bronze.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(once=True)
            .toTable(target_table)
        )

        print(f"✅ Bronze ingestion started for {entity} -> {target_table}")


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    run_bronze_ingestion(spark)
