# Databricks notebook source
# MAGIC %md
# MAGIC ### Golden Layer Transformation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ------------------------------------------------------
# Setup Spark
# ------------------------------------------------------
spark = SparkSession.builder.appName("GoldTransformation").getOrCreate()

# ------------------------------------------------------
# Gold Transformation Function
# ------------------------------------------------------
def run_gold_transformation(spark):
    silver_db = "pyspark_dwh.silver"
    gold_db = "pyspark_dwh.gold"
    checkpoint_base = "/Volumes/pyspark_dwh/gold/checkpoint"

    # Ensure Gold schema and checkpoint volume exist
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_db}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS pyspark_dwh.gold.checkpoint")

    # ---------------------------------
    # Dimension Table: dim_customers
    # ---------------------------------
    print("🚀 Starting Gold transformation for dim_customers")
    df_cust = spark.readStream.format("delta").table(f"{silver_db}.customers")
    df_cust = df_cust.select(
        col("customer_id"),
        col("name"),
        initcap(col("address_city")).alias("city"),
        initcap(col("address_country")).alias("country"),
        col("address_postal_code").alias("postal_code"),
        col("email")
    ).filter(col("customer_id").isNotNull())  # Data quality check
    df_cust = df_cust.withColumn("gold_ingest_ts", current_timestamp())

    # Write dim_customers
    checkpoint_cust = f"{checkpoint_base}/dim_customers"
    target_table_cust = f"{gold_db}.dim_customers"
    (
        df_cust.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_cust)
        .trigger(once=True)
        .toTable(target_table_cust)
    )
    print(f"✅ Gold dim_customers table started -> {target_table_cust}")

    # ---------------------------------
    # Dimension Table: dim_products
    # ---------------------------------
    print("🚀 Starting Gold transformation for dim_products")
    df_prod = spark.read.format("delta").table(f"{silver_db}.products")  # Non-streaming, assumed static
    df_prod = df_prod.select(
        col("product_id"),
        col("product_name"),
        lower(col("category")).alias("category"),
        col("price")
    ).filter((col("product_id").isNotNull()) & (col("price") > 0))  # Data quality check
    df_prod = df_prod.withColumn("gold_ingest_ts", current_timestamp())

    # Write dim_products
    target_table_prod = f"{gold_db}.dim_products"
    (
        df_prod.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table_prod)
    )
    spark.sql(f"OPTIMIZE {target_table_prod} ZORDER BY (product_id)")
    print(f"✅ Gold dim_products table created -> {target_table_prod}")

    # ---------------------------------
    # Fact Table: fact_sales
    # ---------------------------------
    print("🚀 Starting Gold transformation for fact_sales")
    df_ord = spark.readStream.format("delta").table(f"{silver_db}.orders")

    # Read dim tables with aliases
    df_cust_dim = spark.read.format("delta").table(f"{gold_db}.dim_customers").alias("cust")
    df_prod_dim = spark.read.format("delta").table(f"{gold_db}.dim_products").alias("prod")

    # Join orders with dimensions
    df_fact = df_ord.join(
        df_cust_dim,
        df_ord.customer_customer_id == df_cust_dim.customer_id,
        "inner"
    ).join(
        df_prod_dim,
        df_ord.items_item_id == df_prod_dim.product_id,
        "inner"
    )

    # Select final columns with correct order (total_order_value right after quantity)
    df_fact = df_fact.select(
        # Order info
        df_ord.order_id,
        df_ord.timestamp.alias("order_timestamp"),

        # Customer info
        df_ord.customer_customer_id.alias("customer_id"),
        df_ord.customer_name,
        df_ord.customer_email,
        df_cust_dim.city.alias("customer_city"),
        df_cust_dim.country.alias("customer_country"),
        df_cust_dim.postal_code.alias("customer_postal_code"),

        # Payment info
        df_ord.payment_method,
        df_ord.payment_transaction_id,

        # Items info
        df_ord.items_item_id.alias("product_id"),
        df_ord.items_product_name.alias("product_name"),
        df_ord.items_price.alias("price"),
        df_ord.items_quantity.alias("quantity"),
        (df_ord.items_price * df_ord.items_quantity).alias("total_order_value"),  # 👈 inserted right after quantity

        # Metadata info
        df_ord.metadata_key,
        df_ord.metadata_value
    )

    # Add Gold ingestion timestamp
    df_fact = df_fact.withColumn("gold_ingest_ts", current_timestamp())

    # Filter invalid records
    df_fact = df_fact.filter(
        (col("order_id").isNotNull()) &
        (col("quantity") > 0) &
        (col("total_order_value") > 0)
    )

    # Write fact_sales
    checkpoint_fact = f"{checkpoint_base}/fact_sales"
    target_table_fact = f"{gold_db}.fact_sales"
    (
        df_fact.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_fact)
        .trigger(once=True)
        .partitionBy("order_timestamp")
        .toTable(target_table_fact)
    )
    spark.sql(f"OPTIMIZE {target_table_fact} ZORDER BY (customer_id, product_id)")
    print(f"✅ Gold fact_sales table started -> {target_table_fact}")


# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    run_gold_transformation(spark)
