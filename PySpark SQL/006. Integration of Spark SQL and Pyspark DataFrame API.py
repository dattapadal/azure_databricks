# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/retail_db/orders/

# COMMAND ----------

# MAGIC %sql
# MAGIC use database dattadb

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists orders

# COMMAND ----------

orders_df = spark.read.csv (
    'dbfs:/FileStore/tables/retail_db/orders/',
    schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

orders_df.show()

# COMMAND ----------

orders_df.createOrReplaceTempView('orders_v')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

spark.sql(''' 
    select order_status, count(*) as order_count
    from orders_v
    group by all
    order by 2 desc
          ''').show()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select order_status, count(*) as order_count
# MAGIC     from orders_v
# MAGIC     group by all
# MAGIC     order by 2 desc

# COMMAND ----------

orders_df.show()

# COMMAND ----------

from pyspark.sql.functions import count, lit 

# COMMAND ----------

orders_df. \
    groupBy('order_status'). \
    agg(count(lit(1)).alias('order_count')). \
    show()

# COMMAND ----------

order_count_by_status = orders_df. \
                            groupBy('order_status'). \
                            agg(count(lit(1)).alias('order_count'))

# COMMAND ----------

help(order_count_by_status.write.saveAsTable)

# COMMAND ----------

order_count_by_status.write.saveAsTable('dattadb.order_count_by_status', format='parquet', mode='append')

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from order_count_by_status

# COMMAND ----------

order_count_by_status.write.saveAsTable('dattadb.order_count_by_status', format='parquet', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from order_count_by_status

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists order_count_by_status

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table order_count_by_status( 
# MAGIC   order_status STRING, 
# MAGIC   order_count INT
# MAGIC ) using parquet

# COMMAND ----------

orders_df = spark.read.csv (
    'dbfs:/FileStore/tables/retail_db/orders/',
    schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
)

# COMMAND ----------

order_count_by_status = orders_df. \
                            groupBy('order_status'). \
                            agg(count(lit(1)).alias('order_count'))

# COMMAND ----------

help(order_count_by_status.write.insertInto)

# COMMAND ----------

order_count_by_status.write.insertInto('dattadb.order_count_by_status')

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from order_count_by_status

# COMMAND ----------

order_count_by_status.write.insertInto('dattadb.order_count_by_status', overwrite=True)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from order_count_by_status

# COMMAND ----------

order_items_df = spark.read.table('dattadb.order_items').show()

# COMMAND ----------

spark

# COMMAND ----------

spark.catalog.listTables('dattadb')

# COMMAND ----------

for table in spark.catalog.listTables('dattadb'):
    print(table.name)

# COMMAND ----------

for table in spark.catalog.listTables('dattadb'):
    if table.tableType == 'TEMPORARY':
        print(table.name)
        spark.catalog.dropTempView(table.name)

# COMMAND ----------

for table in spark.catalog.listTables('dattadb'):
    if table.tableType == 'TEMPORARY':
        print(table.name)
        
