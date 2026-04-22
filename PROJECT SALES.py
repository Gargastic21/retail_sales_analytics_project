# Databricks notebook source
# DBTITLE 1,create database
# MAGIC %sql
# MAGIC create database project.sales

# COMMAND ----------

# DBTITLE 1,create volme
# MAGIC %sql
# MAGIC create volume project.sales.data

# COMMAND ----------

# DBTITLE 1,read bronze files
raw_orders = spark.read.csv("/Volumes/project/sales/data/bronze/retail_dataset.csv",header=True,inferSchema=True)
display(raw_orders)

# COMMAND ----------

raw_customers = spark.read.json("/Volumes/project/sales/data/bronze/customer_dataset.json")
display(raw_customers)

# COMMAND ----------

display(raw_orders.limit(5))

# COMMAND ----------

# DBTITLE 1,silver- orders
from pyspark.sql.functions import *
spark.conf.set("spark.sql.ansi.enabled", "false")

order_ts = coalesce(
    to_timestamp(trim(col("order_date")), "yyyy-MM-dd"),
    to_timestamp(trim(col("order_date")), "dd/MM/yyyy"),
    to_timestamp(trim(col("order_date")), "d-MMM-yyyy"),
    to_timestamp(trim(col("order_date")), "MM-dd-yyyy"),
    to_timestamp(trim(col("order_date")), "dd-MM-yyyy")
)

clean_orders = (
    raw_orders
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

clean_orders = clean_orders.dropDuplicates(["order_id", "product_id"]).filter(col("order_id").isNotNull() & col("product_id").isNotNull())

# display(clean_orders)

clean_orders.write.format("delta").mode("overwrite").saveAsTable("project.sales.silver_orders")



# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from project.sales.silver_orders

# COMMAND ----------

display(raw_customers.limit(7))

# COMMAND ----------

# DBTITLE 1,clean cstomer
# ---------- SILVER: Customers ----------

signup_ts = coalesce(
    to_timestamp(trim(col("signup_date")), "yyyy-MM-dd"),
    to_timestamp(trim(col("signup_date")), "dd/MM/yyyy"),
    to_timestamp(trim(col("signup_date")), "d-MMM-yyyy"),
    to_timestamp(trim(col("signup_date")), "yyyy/MM/dd")
)

clean_customers = (
    raw_customers
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("gender", lower(trim(col("gender"))))
    .withColumn("age", when(trim(col("age")).isNull() | (trim(col("age"))==""), None).otherwise(col("age")))
    .withColumn("age", when(col("age").cast('int') < 0, None).otherwise(col("age").cast('int')))
    .withColumn("city", trim(col("city")))
    .withColumn("loyalty_tier", lower(trim(col("loyalty_tier"))))
    .withColumn("signup_date", signup_ts)
)

# display(clean_customers)

clean_customers.write.format("delta").mode("overwrite").saveAsTable("project.sales.silver_customers")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project.sales.silver_customers

# COMMAND ----------

# MAGIC %md
# MAGIC gold layer 

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, min as _min, max as _max, avg, countDistinct, coalesce, when, lit, current_timestamp, to_date
silver_orders = spark.table("project.sales.silver_orders")
silver_customers = spark.table("project.sales.silver_customers")

orders_enriched = silver_orders.join(silver_customers, on="customer_id", how="left")

orders_enriched = orders_enriched.withColumn("order_date", to_date(col("order_date_ts"))) \
                                 .withColumn("year", year(col("order_date_ts"))) \
                                 .withColumn("month", month(col("order_date_ts")))

gold_df = (
    orders_enriched
    .groupBy("product_id","product_name","customer_id","category","year","month","order_date","gender","age","city","loyalty_tier")
    .agg(
        _sum("total_amount").alias("total_sales"),
        _sum(coalesce(col("quantity"), lit(0))).alias("total_quantity"),
        countDistinct("order_id").alias("total_orders"),
        _min("price").alias("min_price"),
        _max("price").alias("max_price"),
        avg("price").alias("avg_unit_price"),
        _sum(when(col("returned") == "yes", 1).otherwise(0)).alias("returned_count"),
        _sum(when(col("returned") == "yes", col("total_amount")).otherwise(0.0)).alias("returned_amount")
    )
)

gold_df = gold_df.withColumn("avg_order_value", when(col("total_orders")>0, col("total_sales")/col("total_orders")).otherwise(lit(0.0))) \
                 .withColumn("avg_price_per_item", when(col("total_quantity")>0, col("total_sales")/col("total_quantity")).otherwise(lit(0.0))) \
                 .withColumn("refreshed_at", current_timestamp())

# display(gold_df)

gold_df.write.format("delta").mode("overwrite").saveAsTable("project.sales.gold_aggregates")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project.sales.gold_aggregates

# COMMAND ----------

