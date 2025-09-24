# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold Layer Data Quality cheks
# MAGIC
# MAGIC This script runs data quality checks on Gold layer tables (dim_customers, dim_products, fact_sales) in the Lakehouse.
# MAGIC
# MAGIC It validates:
# MAGIC
# MAGIC Primary keys → no null IDs.
# MAGIC
# MAGIC String fields → no leading/trailing spaces.
# MAGIC
# MAGIC Dates → valid order_timestamp range.
# MAGIC
# MAGIC Numeric fields → no negative or zero values (price, quantity, total_order_value).
# MAGIC
# MAGIC Entity-specific checks → valid email formats (customers), integer quantities (fact_sales).
# MAGIC
# MAGIC Referential integrity → detects orphaned records in fact_sales without matching customers or products.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, to_timestamp, length, lit
from pyspark.sql.types import StringType, DoubleType, IntegerType
from functools import reduce

# ------------------------------------------------------
# Setup Spark
# ------------------------------------------------------
spark = SparkSession.builder.appName("GoldDataQualityCheck").getOrCreate()

# ------------------------------------------------------
# Utility: Check for unwanted spaces in string columns
# ------------------------------------------------------
def check_unwanted_spaces(df, string_cols):
    """
    Identify rows with leading/trailing spaces in specified string columns.
    """
    conditions = [length(col(c)) != length(trim(col(c))) for c in string_cols]
    if conditions:
        return df.filter(reduce(lambda x, y: x | y, conditions))
    return spark.createDataFrame([], df.schema)

# ------------------------------------------------------
# Utility: Check for invalid dates in timestamp columns
# ------------------------------------------------------
def check_invalid_dates(df, date_cols, min_date="1900-01-01", max_date="2100-01-01"):
    """
    Identify rows with invalid or out-of-range dates.
    """
    invalid_conditions = []
    for c in date_cols:
        ts_col = to_timestamp(col(c))
        invalid_conditions.append(
            (ts_col.isNull()) | (ts_col < to_timestamp(lit(min_date))) | (ts_col > to_timestamp(lit(max_date)))
        )
    if invalid_conditions:
        return df.filter(reduce(lambda x, y: x | y, invalid_conditions))
    return spark.createDataFrame([], df.schema)

# ------------------------------------------------------
# Utility: Check for null primary keys
# ------------------------------------------------------
def check_null_pks(df, pk_cols):
    """
    Identify rows with null primary keys.
    """
    conditions = [col(c).isNull() for c in pk_cols]
    if conditions:
        return df.filter(reduce(lambda x, y: x | y, conditions))
    return spark.createDataFrame([], df.schema)

# ------------------------------------------------------
# Utility: Check for negative values in numeric columns
# ------------------------------------------------------
def check_negative_values(df, num_cols):
    """
    Identify rows with negative or zero values in numeric fields (where inappropriate).
    """
    conditions = [col(c) <= 0 for c in num_cols]
    if conditions:
        return df.filter(reduce(lambda x, y: x | y, conditions))
    return spark.createDataFrame([], df.schema)

# ------------------------------------------------------
# Utility: Check for orphaned records in fact table
# ------------------------------------------------------
def check_orphaned_records(df_fact, df_dim, fact_key, dim_key, dim_name):
    """
    Identify fact table records with no matching dimension table record.
    """
    df_orphaned = df_fact.join(
        df_dim,
        df_fact[fact_key] == df_dim[dim_key],
        "left_anti"
    )
    return df_orphaned

# ------------------------------------------------------
# Gold Data Quality Check Function
# ------------------------------------------------------
def run_gold_quality_checks(spark):
    gold_db = "pyspark_dwh.gold"
    quality_db = "pyspark_dwh.quality"
    checkpoint_base = "/Volumes/pyspark_dwh/quality/checkpoint"
    entities = ["dim_customers", "dim_products", "fact_sales"]

    # Entity-specific configurations
    entity_configs = {
        "dim_customers": {
            "pk_cols": ["customer_id"],
            "string_cols": ["name", "city", "country", "postal_code", "email"],
            "date_cols": [],  # No dates in dim_customers
            "num_cols": [],   # No numerics to check
            "extra_checks": lambda df: df.filter(~col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$"))  # Basic email validation
        },
        "dim_products": {
            "pk_cols": ["product_id"],
            "string_cols": ["product_name", "category"],
            "date_cols": [],  # No dates
            "num_cols": ["price"],
            "extra_checks": lambda df: spark.createDataFrame([], df.schema)  # No extra for dim_products
        },
        "fact_sales": {
            "pk_cols": ["order_id"],
            "string_cols": ["customer_name", "customer_email", "customer_city", "customer_country", 
                            "customer_postal_code", "payment_method", "payment_transaction_id", 
                            "product_name", "metadata_key", "metadata_value"],
            "date_cols": ["order_timestamp"],
            "num_cols": ["price", "quantity", "total_order_value"],
            "extra_checks": lambda df: df.filter(col("quantity") % 1 != 0)  # Ensure quantity is integer
        }
    }

    # Ensure Quality schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {quality_db}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS pyspark_dwh.quality.checkpoint")

    # Read dimension tables for orphaned record checks
    df_dim_customers = spark.read.format("delta").table(f"{gold_db}.dim_customers")
    df_dim_products = spark.read.format("delta").table(f"{gold_db}.dim_products")

    for entity in entities:
        print(f"🚀 Starting quality checks for Gold entity: {entity}")

        # Read Gold Delta table (batch mode)
        df_gold = spark.read.format("delta").table(f"{gold_db}.{entity}")

        config = entity_configs.get(entity, {})
        issues = []

        # Check null PKs
        df_null_pks = check_null_pks(df_gold, config.get("pk_cols", []))
        if df_null_pks.count() > 0:
            issues.append(f"❌ {df_null_pks.count()} rows with null primary keys")
            df_null_pks.show(5, truncate=False)

        # Check unwanted spaces
        df_spaces = check_unwanted_spaces(df_gold, config.get("string_cols", []))
        if df_spaces.count() > 0:
            issues.append(f"❌ {df_spaces.count()} rows with unwanted spaces in strings")
            df_spaces.show(5, truncate=False)

        # Check invalid dates
        df_invalid_dates = check_invalid_dates(df_gold, config.get("date_cols", []))
        if df_invalid_dates.count() > 0:
            issues.append(f"❌ {df_invalid_dates.count()} rows with invalid dates")
            df_invalid_dates.show(5, truncate=False)

        # Check negative values
        df_negatives = check_negative_values(df_gold, config.get("num_cols", []))
        if df_negatives.count() > 0:
            issues.append(f"❌ {df_negatives.count()} rows with negative/zero numeric values")
            df_negatives.show(5, truncate=False)

        # Extra entity-specific checks
        df_extra = config.get("extra_checks", lambda df: spark.createDataFrame([], df.schema))(df_gold)
        if df_extra.count() > 0:
            issues.append(f"❌ {df_extra.count()} rows failing extra checks (e.g., invalid email or non-integer quantity)")
            df_extra.show(5, truncate=False)

        # Check for orphaned records (only for fact_sales)
        if entity == "fact_sales":
            # Check for orphaned customer_id
            df_orphaned_customers = check_orphaned_records(df_gold, df_dim_customers, "customer_id", "customer_id", "dim_customers")
            if df_orphaned_customers.count() > 0:
                issues.append(f"❌ {df_orphaned_customers.count()} rows with orphaned customer_id")
                df_orphaned_customers.show(5, truncate=False)

            # Check for orphaned product_id
            df_orphaned_products = check_orphaned_records(df_gold, df_dim_products, "product_id", "product_id", "dim_products")
            if df_orphaned_products.count() > 0:
                issues.append(f"❌ {df_orphaned_products.count()} rows with orphaned product_id")
                df_orphaned_products.show(5, truncate=False)

        # If issues found, write to quality table
        if issues:
            print("\n".join(issues))
            # Combine all issue DFs and add metadata
            df_all_issues = df_null_pks.union(df_spaces).union(df_invalid_dates).union(df_negatives).union(df_extra)
            if entity == "fact_sales":
                df_all_issues = df_all_issues.union(df_orphaned_customers).union(df_orphaned_products)
            df_all_issues = df_all_issues.withColumn("entity", lit(entity)).withColumn("check_ts", current_timestamp())

            target_table = f"{quality_db}.quality_issues"
            (
                df_all_issues.write.format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )
            print(f"📝 Quality issues logged to {target_table}")
        else:
            print(f"✅ No quality issues found for {entity}")

# ------------------------------------------------------
# Main
# ------------------------------------------------------
if __name__ == "__main__":
    run_gold_quality_checks(spark)