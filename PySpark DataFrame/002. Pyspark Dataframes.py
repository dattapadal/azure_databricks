# Databricks notebook source
spark

# COMMAND ----------

spark.read.

# COMMAND ----------

# MAGIC     %fs ls 'dbfs:/FileStore/tables/retail_db/orders'

# COMMAND ----------

spark.read.csv('dbfs:/FileStore/tables/retail_db/orders').show()

# COMMAND ----------

display(spark.read.\
    csv('dbfs:/FileStore/tables/retail_db/orders', schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING'))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select current_user()

# COMMAND ----------

df = spark.read.\
    csv('dbfs:/FileStore/tables/retail_db/orders', schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING')

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

display(df.select('*'))

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

display(df.select('order_id', 'order_status'))

# COMMAND ----------

display(df.select('order_status').distinct())

# COMMAND ----------

display(df.select('order_date', 'order_status').distinct()) 

# COMMAND ----------

display(
    df.select('order_date', 'order_status').distinct().orderBy('order_date', 'order_status')
) 

# COMMAND ----------

df.select('order_date', 'order_status'). \
    distinct(). \
    orderBy('order_date', 'order_status'). \
    count()

# COMMAND ----------

help(df.drop)

# COMMAND ----------

display(df)

# COMMAND ----------

display(
    df.drop('order_customer_id')
)

# COMMAND ----------

from pyspark.sql import functions as f  

# COMMAND ----------

help(f.date_format)

# COMMAND ----------

from pyspark.sql.functions import date_format, cast

# COMMAND ----------

df.select('order_id', 
          'order_date', 
          cast(int, date_format('order_date', 'yyyyMM')).alias('order_year_mth')
).show()

# COMMAND ----------

df.withColumn('order_year_mth', cast(int, date_format('order_date', 'yyyyMM'))).show()

# COMMAND ----------

df.withColumn('order_date', cast(int, date_format('order_date', 'yyyyMM'))).show() # replaces the existing column if the name matches

# COMMAND ----------

df.\
    drop('order_customer_id'). \
    withColumn('order_year_mth', cast(int, date_format('order_date', 'yyyyMM'))).show() 

# COMMAND ----------

df.write

# COMMAND ----------

df.write.format('delta').save('dbfs:/FileStore/tables/retail_db/orders/output/')

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/retail_db/orders/output/'

# COMMAND ----------

spark.read.format('delta').load('dbfs:/FileStore/tables/retail_db/orders/output/').show()

# COMMAND ----------


