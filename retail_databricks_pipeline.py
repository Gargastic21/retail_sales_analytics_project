
# retail_databricks_pipeline.py
# End-to-end Bronze -> Silver -> Gold pipeline for Databricks (PySpark).
# Designed to run in Databricks notebook (copy cells into notebook) or as a PySpark script with adjustments.
from pyspark.sql.functions import (
    trim, lower, to_timestamp, coalesce, translate, when, lit, col,
    sum as _sum, countDistinct, avg, min as _min, max as _max,
    first, current_timestamp, current_date, datediff, year, month, to_date
)
from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS retail_demo")
spark.sql("USE retail_demo")

# Paths
csv_path = "/FileStore/retail/retail_orders.csv"  # if running in Databricks upload CSV here OR update to /mnt/data/retail_dataset.csv
# If running locally with Spark, you can set csv_path = "/mnt/data/retail_dataset.csv"

bronze_parquet_path = "/tmp/retail/bronze_parquet"

# BRONZE: ingest raw CSV
raw_df = spark.read.option("header","true").option("inferSchema","false").csv(csv_path)
raw_df.write.mode("overwrite").parquet(bronze_parquet_path)
spark.read.parquet(bronze_parquet_path).write.format("delta").mode("overwrite").saveAsTable("retail_demo.bronze_orders")

# SILVER: cleaning
bronze = spark.table("retail_demo.bronze_orders")
order_ts = coalesce(
    to_timestamp(trim(col("order_date")), "yyyy-MM-dd"),
    to_timestamp(trim(col("order_date")), "dd/MM/yyyy"),
    to_timestamp(trim(col("order_date")), "d-MMM-yyyy"),
    to_timestamp(trim(col("order_date")), "MM-dd-yyyy"),
    to_timestamp(trim(col("order_date")), "dd-MM-yyyy")
)

clean_df = (
    bronze
    .withColumn("order_id", trim(col("order_id")).cast("long"))
    .withColumn("order_date_ts", order_ts)
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("product_name", lower(trim(col("product_name"))))
    .withColumn("category", lower(trim(col("category"))))
    .withColumn("quantity", when(trim(col("quantity")).isNull() | (trim(col("quantity"))==""), lit(1)).otherwise(trim(col("quantity"))))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("quantity", when(col("quantity") <= 0, lit(1)).otherwise(col("quantity")))
    .withColumn("price_clean", translate(trim(col("price")), "$,", ""))
    .withColumn("price", col("price_clean").cast("double"))
    .drop("price_clean")
    .withColumn("payment_type", lower(trim(col("payment_type"))))
    .withColumn("order_status", lower(trim(col("order_status"))))
    .withColumn("returned", lower(trim(col("returned"))))
    .withColumn("total_amount", (coalesce(col("quantity"), lit(0)) * coalesce(col("price"), lit(0.0))).cast("double"))
)

clean_df = clean_df.dropDuplicates(["order_id", "product_id"]).filter(col("order_id").isNotNull() & col("product_id").isNotNull())
clean_df.write.format("delta").mode("overwrite").saveAsTable("retail_demo.silver_orders")

# GOLD: aggregate (product x customer x year x month x date)
silver = spark.table("retail_demo.silver_orders")
silver2 = silver.withColumn("order_date", to_date(col("order_date_ts"))).withColumn("year", year(col("order_date_ts"))).withColumn("month", month(col("order_date_ts")))

gold_df = (
    silver2
    .groupBy("product_id","product_name","customer_id","category","year","month","order_date")
    .agg(
        _sum("total_amount").alias("total_sales"),
        _sum(coalesce(col("quantity"), lit(0))).alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        _min("price").alias("min_price"),
        _max("price").alias("max_price"),
        avg("price").alias("avg_unit_price"),
        _min("order_date_ts").alias("first_order_date"),
        _max("order_date_ts").alias("last_order_date"),
        _sum(when(col("returned") == "yes", 1).otherwise(0)).alias("returned_count"),
        _sum(when(col("returned") == "yes", col("total_amount")).otherwise(0.0)).alias("returned_amount"),
        countDistinct("payment_type").alias("distinct_payment_types"),
        first(col("customer_name"), True).alias("customer_name")
    )
)

gold_df = gold_df.withColumn("avg_order_value", when(col("total_orders")>0, col("total_sales")/col("total_orders")).otherwise(lit(0.0))) \
                 .withColumn("avg_price_per_item", when(col("total_quantity")>0, col("total_sales")/col("total_quantity")).otherwise(lit(0.0))) \
                 .withColumn("days_since_last_order", when(col("last_order_date").isNotNull(), datediff(current_date(), col("last_order_date"))).otherwise(lit(None))) \
                 .withColumn("refreshed_at", current_timestamp())

gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_demo.gold_aggregates")

# KPIs (from silver to avoid double-counting)
overall = spark.table("retail_demo.silver_orders").agg(
    _sum("total_amount").alias("total_revenue"),
    countDistinct("order_id").alias("total_orders"),
    _sum(coalesce(col("quantity"), lit(0))).alias("total_items"),
    _sum(when(col("returned")=="yes", col("total_amount")).otherwise(0.0)).alias("total_returned_amount"),
    countDistinct("customer_id").alias("total_customers")
).collect()[0]

total_revenue = float(overall["total_revenue"] or 0.0)
total_orders = int(overall["total_orders"] or 0)
total_items = int(overall["total_items"] or 0)
total_returned_amount = float(overall["total_returned_amount"] or 0.0)
total_customers = int(overall["total_customers"] or 0)

avg_order_value = (total_revenue / total_orders) if total_orders>0 else 0.0
avg_price_per_item = (total_revenue / total_items) if total_items>0 else 0.0
avg_items_per_order = (total_items / total_orders) if total_orders>0 else 0.0

kpi_row = [{
    "total_revenue": total_revenue,
    "total_orders": total_orders,
    "total_items": total_items,
    "total_customers": total_customers,
    "avg_order_value": avg_order_value,
    "avg_price_per_item": avg_price_per_item,
    "avg_items_per_order": avg_items_per_order,
    "refreshed_at": current_timestamp()
}]

spark.createDataFrame(kpi_row).write.format("delta").mode("overwrite").saveAsTable("retail_demo.gold_kpis_final")

print("Pipeline finished. Tables created: retail_demo.bronze_orders, retail_demo.silver_orders, retail_demo.gold_aggregates, retail_demo.gold_kpis_final")
