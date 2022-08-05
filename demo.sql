-- Databricks notebook source
-- MAGIC %fs
-- MAGIC ls /databricks-datasets/retail-org/customers/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC data_path = "/databricks-datasets/retail-org/customers/"
-- MAGIC # df = spark.read.csv(path=data_path, header=True, inferSchema=True)
-- MAGIC df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(data_path)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC df2 = df.groupBy("region").agg(countDistinct("customer_id").alias("cust_cnt"))
-- MAGIC display(df2)
