# Retail Sales Analysis Project using Medallion Architecture in Databricks

# Description
A mid-size retail company is struggling to consolidate and analyze its sales transactions that are spread across different sources and formats.
The business wants a unified analytics solution to:

clean messy data (inconsistent formats, missing values, duplicates),

build a single source of truth for sales, customers, and products,

monitor KPIs such as revenue, return rate, repeat customers, average order value,

enable business teams to explore insights across product, category, customer, and time dimensions,

and use dashboards for data-driven decisions (e.g., inventory, pricing, promotions).

Databricks Free Edition with a Medallion Architecture (Bronze → Silver → Gold) is chosen to demonstrate how raw retail data can be transformed into actionable insights.

# Business problem statement
The retail business faces the following challenges:

Messy Transaction Data

Dates recorded in multiple formats (yyyy-MM-dd, dd/MM/yyyy, etc.)

Price fields with mixed symbols ($19.99, 19.99, missing values)

Inconsistent product names (T-shirt XL, tshirt xl)

Duplicates and missing customer IDs

Limited Visibility into KPIs

No clear picture of total revenue, orders, or customer lifetime value (CLV)

Unable to track returns, refund impact, or return rates

No insights on monthly growth trends, top products, or category performance

Difficulty in Customer Analysis

Hard to identify repeat vs one-time customers

No way to measure purchase frequency or churn risk

Lack of understanding of product-customer associations

Manual Reporting Pain Points

Business teams spend days cleaning spreadsheets

Insights are delayed and not real-time

Decisions (inventory, promotions, pricing) are reactive, not proactive



# Proposed Solution (Your Project)

Bronze Layer: Ingest messy CSV/Parquet files manually (as simulated in Databricks free edition).

Silver Layer: Clean and standardize data (fix dates, remove $ from prices, normalize product names, handle missing/negative quantities, remove duplicates).

Gold Layer: Create one rich aggregate Delta table with grouped metrics across product, customer, category, and time (year, month, date).

KPIs Derived:

Total Revenue, Total Orders, Total Items Sold

Average Order Value (AOV), Average Price per Item

Repeat Customer Rate, Purchase Frequency

Return Rate (by count & value)

Monthly Sales Trends & MoM Growth

Top Products by Revenue and Quantity

Customer Lifetime Value (CLV) approximation

Dashboard: Build a Databricks SQL Dashboard showing trends, category performance, top products, and customer insights.

## Steps

1. login Databricks
2. create a catalog by the name: retail_sales_project_catalog
   And below folder and file structure

```bash
retail_sales_project_catalog/
---retail_sales_db/
------retail_sales_volume/
---------bronze_directory/
```

3. upload files to the bronze directory

```bash
/Volumes/retail_sales_project_catalog/retail_sales_db/retail_sales_volume/bronze_directory/customer_dataset.json

/Volumes/retail_sales_project_catalog/retail_sales_db/retail_sales_volume/bronze_directory/retail_dataset.csv
```

4. Create a folder in workspace and notebook

```bash
/Workspace/Users/amishagarg373@gmail.com/retail_sales_project_folder/retail_sales_project_notebook
```

## notebook code

```python

%sql
create database retail_sales_project_catalog.retail_sales_db

%sql
create volume retail_sales_project_catalog.retail_sales_db.retail_sales_volume
```
```python
# DBTITLE 1,read bronze files
raw_orders = spark.read.csv("/Volumes/retail_sales_project_catalog/retail_sales_db/retail_sales_volume/bronze_directory/retail_dataset.csv",header=True,inferSchema=True)
display(raw_orders)

# COMMAND ----------

raw_customers = spark.read.json("/Volumes/retail_sales_project_catalog/retail_sales_db/retail_sales_volume/bronze_directory/customer_dataset.json")
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

clean_orders.write.format("delta").mode("overwrite").saveAsTable("retail_sales_project_catalog.retail_sales_db.silver_orders_table")

```
```python
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

clean_customers.write.format("delta").mode("overwrite").saveAsTable("retail_sales_project_catalog.retail_sales_db.silver_customers_table")
```

```python
# MAGIC gold layer 

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, min as _min, max as _max, avg, countDistinct, coalesce, when, lit, current_timestamp, to_date, col, year, month
silver_orders = spark.table("retail_sales_project_catalog.retail_sales_db.silver_orders_table")
silver_customers = spark.table("retail_sales_project_catalog.retail_sales_db.silver_customers_table")

orders_enriched = silver_orders.join(silver_customers, on="customer_id", how="left")

orders_enriched = orders_enriched.withColumn("order_date", to_date(col("order_date_ts"))) \
                                 .withColumn("year", year(col("order_date_ts"))) \
                                 .withColumn("month", month(col("order_date_ts")))

display(orders_enriched)
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

display(gold_df)

gold_df.write.format("delta").mode("overwrite").saveAsTable("retail_sales_project_catalog.retail_sales_db.gold_aggregate_table")
```
```bash
this will create tables in retail_sales_db
```

```bash
After this create dashboard: go to Dashboards---> create dashboard--->add dataset---> select the gold_agg_table file----> add visualization--->publish
```


## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
