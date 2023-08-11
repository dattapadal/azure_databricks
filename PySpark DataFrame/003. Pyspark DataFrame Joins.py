# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/retail_db/orders

# COMMAND ----------

orders_df = spark. \
                read. \
                csv (
                    'dbfs:/FileStore/tables/retail_db/orders',
                    schema = 'order_id INT, order_date DATE, order_customer_id INT, order_status STRING'
                )
             

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.count()

# COMMAND ----------

orders_df

# COMMAND ----------

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

display(order_items_df)

# COMMAND ----------

order_items_df.printSchema()

# COMMAND ----------

order_items_df.count()

# COMMAND ----------

type(orders_df)

# COMMAND ----------

help(orders_df.join)

# COMMAND ----------

 order_details_df = orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'])

# COMMAND ----------

 order_details_df = orders_df.join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'inner') #default is inner anyway we can explicitely state it

# COMMAND ----------

display(order_details_df)

# COMMAND ----------

order_details_df. \
    select('order_id', 'order_date', 'order_customer_id', 'order_item_subtotal'). \
    show()

# COMMAND ----------

order_details_df. \
    select(orders_df['*'], 'order_item_subtotal'). \
    show()

# COMMAND ----------

order_details_df. \
    select(orders_df['*'], order_items_df['order_item_subtotal']). \
    show()

# COMMAND ----------

# Compute daily revenue for orders which are placed in Jan 2014 and also the order status is either COMPLETE or CLOSED.
orders_df.filter("order_status in ('COMPLETE', 'CLOSED') and date_format(order_date, 'yyyyMM') = 201401 ").show()

# COMMAND ----------

from pyspark.sql.functions import sum, col, round

# COMMAND ----------

orders_df. \
    filter("order_status in ('COMPLETE', 'CLOSED') and date_format(order_date, 'yyyyMM') = 201401 "). \
    join(order_items_df, orders_df['order_id']==order_items_df['order_item_order_id']). \
    groupBy('order_date'). \
    agg(round(sum('order_item_subtotal'),2).alias('revenue')). \
    orderBy(col('revenue').desc()). \
    show()

# COMMAND ----------

orders_df.select('order_id').distinct().count()

# COMMAND ----------

order_items_df.select('order_item_order_id').distinct().count()

# COMMAND ----------

#find the records that are present in orders_df but not in order_items_df 
orders_df. \
    join(order_items_df, orders_df['order_id'] == order_items_df['order_item_order_id'], 'left'). \
    select(orders_df['*'], order_items_df['order_item_subtotal']). \
    filter(order_items_df['order_item_subtotal'].isNull() ). \
    show()

# COMMAND ----------


