# Databricks notebook source
# MAGIC %python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC 
# MAGIC data_path = "/databricks-datasets/retail-org/customers/"
# MAGIC @dlt.table(
# MAGIC   comment="retail-org customer dataset"
# MAGIC )
# MAGIC def cust_raw():
# MAGIC   return (spark.read.format("csv").option("header", True).option("inferSchema", True).load(data_path))
# MAGIC 
# MAGIC @dlt.table(
# MAGIC   comment="customer data cleaned and prepared for analysis."
# MAGIC )
# MAGIC @dlt.expect("valid_tax_id", "tax_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("valid_reg_city", "region is not null")
# MAGIC def cust_prepared():
# MAGIC   return (
# MAGIC     dlt.read("cust_raw")
# MAGIC        .select("tax_id", "region", "city", "customer_id")
# MAGIC   )
# MAGIC 
# MAGIC @dlt.table(
# MAGIC   comment="customer group by region"
# MAGIC )
# MAGIC def cust_agg():
# MAGIC   return (
# MAGIC     dlt.read("cust_prepared")
# MAGIC       .filter("city is null")
# MAGIC       .groupBy("region")
# MAGIC       .agg(countDistinct("customer_id").alias("cust_cnt"))
# MAGIC   )
