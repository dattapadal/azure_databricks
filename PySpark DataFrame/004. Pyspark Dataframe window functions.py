# Databricks notebook source
orders_df = spark. \
                read. \
                csv (
                    'dbfs:/FileStore/tables/retail_db/orders',
                    schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
                )
             
order_items_df = spark. \
                    read. \
                    csv (
                        'dbfs:/FileStore/tables/retail_db/order_items',
                        schema = '''
                            order_item_id INT, 
                            order_item_order_id INT,
                            order_item_product_id INT,
                            order_item_quantity INT, 
                            order_item_subtotal FLOAT, 
                            order_item_product_price FLOAT
                            '''
                    )

# COMMAND ----------

orders_df.show()

# COMMAND ----------

order_items_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, sum, round

# COMMAND ----------

daily_revenue_df = orders_df. \
                        filter("order_status in ('COMPLETE', 'CLOSED')"). \
                        join(order_items_df, orders_df['order_id']==order_items_df['order_item_order_id']). \
                        groupBy('order_date'). \
                        agg(round(sum('order_item_subtotal'),2).alias('revenue'))

# COMMAND ----------

daily_revenue_df.show()

# COMMAND ----------

daily_revenue_df.count()

# COMMAND ----------

daily_product_revenue_df = orders_df. \
                            filter("order_status in ('COMPLETE', 'CLOSED')"). \
                            join(order_items_df, orders_df['order_id']==order_items_df['order_item_order_id']). \
                            groupBy(orders_df['order_date'], order_items_df['order_item_product_id'] ). \
                            agg(round(sum('order_item_subtotal'),2).alias('revenue'))

# COMMAND ----------

daily_product_revenue_df.show()

# COMMAND ----------

daily_product_revenue_df.count()

# COMMAND ----------

from pyspark.sql.functions import dense_rank, col, rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

 daily_product_revenue_df. \
    filter("order_date = '2014-01-01'"). \
    withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))). \
    show()

# COMMAND ----------

 daily_product_revenue_df. \
    filter("order_date = '2014-01-01'"). \
    withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))). \
    withColumn('rnk', rank().over(Window.orderBy(col('revenue').desc()))). \
    show()

# COMMAND ----------

 daily_product_revenue_df. \
    filter("order_date = '2014-01-01'"). \
    withColumn('drnk', dense_rank().over(Window.orderBy(col('revenue').desc()))). \
    withColumn('rnk', rank().over(Window.orderBy(col('revenue').desc()))). \
    filter('drnk <= 5'). \
    show()

# COMMAND ----------

  daily_product_revenue_df. \
    withColumn('drnk', dense_rank().over(Window.partitionBy('order_date').orderBy(col('revenue').desc()))). \
    filter('drnk <= 5'). \
    show()

# COMMAND ----------

spec = Window.partitionBy('order_date').orderBy(col('revenue').desc())

# COMMAND ----------

spec

# COMMAND ----------

  daily_product_revenue_df. \
    withColumn('drnk', dense_rank().over(spec)). \
    filter('drnk <= 5'). \
    show()

# COMMAND ----------


