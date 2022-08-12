-- Databricks notebook source
set spark.sql.autoBroadcastJoinThreshold=-1;

-- COMMAND ----------

SELECT t1.id, t1.name, t2.id as id2, t2.name as name2
FROM    demo.user1 t1
JOIN    demo.user2 t2
ON      t1.name = t2.name;

-- COMMAND ----------

SELECT t1.id, t1.name, t2.id as id2, t2.name as name2
FROM    demo.user1_bucket t1
JOIN    demo.user2_bucket t2
ON      t1.name = t2.name;

-- COMMAND ----------

select *, input_file_name(), input_file_block_start(), input_file_block_length() from demo.user1_bucket;

-- COMMAND ----------

set spark.sql.sources.bucketing.enabled

-- COMMAND ----------

select *, input_file_name(), input_file_block_start(), input_file_block_length() from demo.user2_bucket;

-- COMMAND ----------

select *, input_file_name(), input_file_block_start(), input_file_block_length() from demo.user2;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE demo.user1_bucket (
-- MAGIC   `id` BIGINT,
-- MAGIC   `name` STRING)
-- MAGIC USING parquet
-- MAGIC CLUSTERED BY (name)
-- MAGIC SORTED BY (name)
-- MAGIC INTO 10 BUCKETS;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE demo.user2_bucket (
-- MAGIC   `id` BIGINT,
-- MAGIC   `name` STRING)
-- MAGIC USING parquet
-- MAGIC CLUSTERED BY (name)
-- MAGIC SORTED BY (name)
-- MAGIC INTO 10 BUCKETS;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- insert overwrite table demo.user1_bucket select * from demo.user1;
-- MAGIC insert overwrite table demo.user2_bucket select * from demo.user2;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC desc formatted demo.user2_bucket;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC desc detail demo.user1

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /user/hive/warehouse/demo.db/user2_bucket

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE demo.user1
-- MAGIC SELECT id, concat("user-",cast(rand()*100 as int)) as name 
-- MAGIC FROM   range(1, 101);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT OVERWRITE TABLE demo.user2
-- MAGIC SELECT id, concat("user-",cast(rand()*100 as int)) as name 
-- MAGIC FROM   range(1, 101);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- use demo;
-- MAGIC CREATE TABLE `demo`.`user1` (
-- MAGIC   `id` BIGINT,
-- MAGIC   `name` STRING)
-- MAGIC USING parquet
-- MAGIC -- CLUSTERED BY (name)
-- MAGIC -- SORTED BY (name)
-- MAGIC -- INTO 10 BUCKETS
-- MAGIC ;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE `demo`.`user2` (
-- MAGIC   `id` BIGINT,
-- MAGIC   `name` STRING)
-- MAGIC USING parquet

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show tables;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- show databases;
-- MAGIC create database demo;
