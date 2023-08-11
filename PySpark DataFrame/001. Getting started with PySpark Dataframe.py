# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/retail_db/schemas.json

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text.`dbfs:/FileStore/tables/retail_db/schemas.json`

# COMMAND ----------

help(spark.read.text)

# COMMAND ----------

spark.read.text('dbfs:/FileStore/tables/retail_db/schemas.json', wholetext=True).show(truncate=False)

# COMMAND ----------

spark.read.text('dbfs:/FileStore/tables/retail_db/schemas.json', wholetext=True).first()

# COMMAND ----------

spark.read.text('dbfs:/FileStore/tables/retail_db/schemas.json', wholetext=True).first().value

# COMMAND ----------

schemas_text = spark.read.text('dbfs:/FileStore/tables/retail_db/schemas.json', wholetext=True).first().value

# COMMAND ----------

type(schemas_text)

# COMMAND ----------

import json

# COMMAND ----------

json.loads(schemas_text)

# COMMAND ----------

json.loads(schemas_text).keys()

# COMMAND ----------

json.loads(schemas_text)['orders']

# COMMAND ----------

column_details = json.loads(schemas_text)['orders']

# COMMAND ----------

type(column_details)

# COMMAND ----------

sorted(column_details, key=lambda col: col['column_position'])

# COMMAND ----------

[col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]

# COMMAND ----------

column_names = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]

# COMMAND ----------

[col['data_type'] for col in sorted(column_details, key=lambda col: col['column_position'])]

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/retail_db/orders

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from csv.`dbfs:/FileStore/tables/retail_db/orders`

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders', inferSchema=True)

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders', inferSchema=True).toDF('order_id', 'order_date', 'order_customer_id', 'order_status')

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders', inferSchema=True).toDF(*column_names)

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders', inferSchema=True).toDF(*column_names).show()

# COMMAND ----------

orders = spark.read.csv('dbfs:/FileStore/tables/retail_db/orders', inferSchema=True).toDF(*column_names)

# COMMAND ----------

orders.show()

# COMMAND ----------

orders. \
    write. \
    mode('overwrite'). \
    parquet('dbfs:/FileStore/tables/retail_db_parquet/orders')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/retail_db_parquet/orders'

# COMMAND ----------

from pyspark.sql.functions import count, col

# COMMAND ----------

orders. \
    groupBy('order_status'). \
    agg(count('*').alias('order_count')). \
    orderBy(col('order_count')). \
    show()

# COMMAND ----------

orders. \
    groupBy('order_status'). \
    agg(count('*').alias('order_count')). \
    orderBy(col('order_count').desc()). \
    show()

# COMMAND ----------

# consolidating all the above snippets into a modular code
import json 
def get_columns(schemas_file, ds_name):
    schema_text = spark.read.text(schemas_file, wholetext=True).first().value
    schemas = json.loads(schema_text)
    column_details = schemas[ds_name]
    columns = [col['column_name'] for col in sorted(column_details, key=lambda col: col['column_position'])]
    return columns 


# COMMAND ----------

get_columns('dbfs:/FileStore/tables/retail_db/schemas.json', 'orders')

# COMMAND ----------

get_columns('dbfs:/FileStore/tables/retail_db/schemas.json', 'order_items')

# COMMAND ----------

list(json.loads(schemas_text).keys())

# COMMAND ----------

ds_list = list(json.loads(schemas_text).keys())

# COMMAND ----------

ds_list

# COMMAND ----------

base_dir = 'dbfs:/FileStore/tables/retail_db'

# COMMAND ----------

for ds in ds_list:
    print(f'Processing {ds} data')
    columns = get_columns(f'{base_dir}/schemas.json', ds)
    print(columns)


# COMMAND ----------

target_dir = 'dbfs:/FileStore/tables/retail_db_parquet'

# COMMAND ----------

for ds in ds_list:
    print(f'Processing {ds} data')
    columns = get_columns(f'{base_dir}/schemas.json', ds)
    df = spark.read.csv(f'{base_dir}/{ds}', inferSchema=True).toDF(*columns)
    df. \
        write. \
        mode('overwrite'). \
        parquet(f'{target_dir}/{ds}')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/retail_db_parquet'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/retail_db_parquet/order_items'

# COMMAND ----------


