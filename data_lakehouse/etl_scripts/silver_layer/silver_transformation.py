# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver Layer Transformation
# MAGIC
# MAGIC The silver_transformation.py script is using PySpark to transform streaming data from the Bronze layer to the Silver layer in a data lakehouse. It processes raw data from Bronze Delta tables (customers, products, orders) in the pyspark_dwh.bronze schema, refining it into cleaner, deduplicated Silver Delta tables in the pyspark_dwh.silver schema.
# MAGIC
# MAGIC Key actions include:
# MAGIC
# MAGIC - Flattening Data: The flatten_df function simplifies nested StructType columns and explodes arrays of structs for a tabular format.
# MAGIC
# MAGIC - Transformation Pipeline: The run_silver_streaming function reads streaming Bronze data, flattens the payload column, deduplicates records using entity-specific primary keys (customer_id, product_id, order_id), adds a silver_ingest_ts timestamp, and writes to Silver tables with checkpoints for reliability.
# MAGIC
# MAGIC - Execution: Uses trigger(once=True) for batch processing, configurable for continuous streaming.
# MAGIC
# MAGIC The script ensures the Silver schema and checkpoint volume exist, supporting the medallion architecture by preparing data for analytics or further Gold-layer processing.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, current_timestamp, explode
from pyspark.sql.types import StructType, ArrayType

# ------------------------------------------------------
# Setup Spark
# ------------------------------------------------------
spark = SparkSession.builder.appName("SilverStreaming").getOrCreate()

# ------------------------------------------------------
# Utility: Flatten nested structs and arrays of structs
# ------------------------------------------------------
def flatten_df(df):
    """
    Recursively flatten all struct columns and explode arrays of structs.
    """
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        array_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType) 
                      and isinstance(f.dataType.elementType, StructType)]

        if not struct_cols and not array_cols:
            break

        # Flatten struct columns
        for s in struct_cols:
            expanded = [col(f"{s}.{c}").alias(f"{s}_{c}") for c in df.select(f"{s}.*").columns]
            df = df.select("*", *expanded).drop(s)

        # Explode array of structs
        for a in array_cols:
            df = df.withColumn(a, explode(col(a)))

    return df

# ------------------------------------------------------
# Silver Streaming Transformation Function
# ------------------------------------------------------
def run_silver_streaming(spark):
    bronze_db = "pyspark_dwh.bronze"
    silver_db = "pyspark_dwh.silver"
    checkpoint_base = "/Volumes/pyspark_dwh/silver/checkpoint"
    entities = ["customers", "products", "orders"]

    # Define primary keys per entity
    primary_keys_dict = {
        "customers": ["customer_id"],   # adjust according to your JSON schema
        "products": ["product_id"],
        "orders": ["order_id"]
    }

    # Ensure Silver schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_db}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS pyspark_dwh.silver.checkpoint")

    for entity in entities:
        print(f"🚀 Starting Silver streaming transformation for entity: {entity}")

        # Read Bronze Delta table as streaming source
        df_bronze_stream = spark.readStream.format("delta").table(f"{bronze_db}.{entity}")

        # Flatten payload, including arrays of structs
        df_flat = flatten_df(df_bronze_stream.select("payload.*", "ingest_file", "bronze_ingest_ts"))

        # Deduplicate by entity-specific primary keys
        primary_keys = primary_keys_dict.get(entity, [])
        available_keys = [k for k in primary_keys if k in df_flat.columns]

        if available_keys:
            df_dedup = df_flat.dropDuplicates(available_keys)
        else:
            print(f"⚠️ No valid primary key found for {entity}, skipping deduplication")
            df_dedup = df_flat

        # Add Silver ingestion timestamp
        df_final = df_dedup.withColumn("silver_ingest_ts", current_timestamp())

        # Streaming write to Silver Delta table
        checkpoint = f"{checkpoint_base}/{entity}"
        target_table = f"{silver_db}.{entity}"

        (
            df_final.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(once=True)  # change to .trigger(processingTime='1 minute') for continuous streaming
            .toTable(target_table)
        )

        print(f"✅ Silver streaming table started for {entity} -> {target_table}")

# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    run_silver_streaming(spark)
