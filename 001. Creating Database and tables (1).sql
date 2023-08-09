-- Databricks notebook source
Select current_database()

-- COMMAND ----------

drop database if exists dattadb

-- COMMAND ----------

create database dattadb

-- COMMAND ----------

use dattadb

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create table orders (
  order_id BIGINT, 
  order_date STRING, 
  order_customer_id BIGINT, 
  order_status STRING
) using delta

-- COMMAND ----------

desc  orders

-- COMMAND ----------

describe formatted orders

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/dattadb.db/orders

-- COMMAND ----------

show create table orders

-- COMMAND ----------

-- MAGIC %fs ls "/FileStore/tables/retail_db_json/orders/"

-- COMMAND ----------

-- MAGIC %fs rm "/FileStore/tables/retail_db_json/orders/part_r_00000_990f5773_9005_49ba_b670_631286032674-1"

-- COMMAND ----------

-- MAGIC %fs head "/FileStore/tables/retail_db_json/orders/part_r_00000_990f5773_9005_49ba_b670_631286032674" 

-- COMMAND ----------

Select * from json.`/FileStore/tables/retail_db_json/orders/`

-- COMMAND ----------

COPY INTO orders
FROM '/FileStore/tables/retail_db_json/orders/'
FILEFORMAT=JSON 


-- COMMAND ----------

Select count(*) from orders

-- COMMAND ----------

desc formatted orders

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/dattadb.db/orders"

-- COMMAND ----------

Select * from orders

-- COMMAND ----------

show tables

-- COMMAND ----------

create table order_items (
  order_item_id bigint,
  order_item_order_id bigint, 
  order_item_product_id bigint,
  order_item_quantity bigint,
  order_item_subtotal double, 
  order_item_product_price double
) using delta 
options (
  path = "dbfs:/user/hive/warehouse/dattadb.db/order_items"
)

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/dattadb.db/order_items"

-- COMMAND ----------

desc formatted order_items

-- COMMAND ----------

select 
  *
    from json.`/FileStore/tables/retail_db_json/order_items/`

-- COMMAND ----------

select 
  order_item_id, 
  order_item_order_id, 
  order_item_product_id, 
  order_item_quantity, 
  order_item_subtotal,
  order_item_product_price
from json.`/FileStore/tables/retail_db_json/order_items/`

-- COMMAND ----------

insert into order_items
select 
  order_item_id, 
  order_item_order_id, 
  order_item_product_id, 
  order_item_quantity, 
  order_item_subtotal,
  order_item_product_price
from json.`/FileStore/tables/retail_db_json/order_items/`

-- COMMAND ----------

-- MAGIC %fs ls "/FileStore/tables/retail_db_json/order_items/"

-- COMMAND ----------

-- MAGIC %fs rm "dbfs:/FileStore/tables/retail_db_json/order_items/part_r_00000_6b83977e_3f20_404b_9b5f_29376ab1419e-1"

-- COMMAND ----------

truncate table order_items

-- COMMAND ----------

insert into order_items
select 
  order_item_id, 
  order_item_order_id, 
  order_item_product_id, 
  order_item_quantity, 
  order_item_subtotal,
  order_item_product_price
from json.`/FileStore/tables/retail_db_json/order_items/`

-- COMMAND ----------

desc formatted order_items

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/user/hive/warehouse/dattadb.db/order_items"

-- COMMAND ----------

select * from order_items

-- COMMAND ----------

select order_item_order_id, count(*) as order_item_count,  round(sum(order_item_subtotal), 2) as order_revenue
from order_items
group by 1
order by 1

-- COMMAND ----------

show tables

-- COMMAND ----------

desc order_items

-- COMMAND ----------

-- CRUD

create table crud_demo (
  user_id INT, 
  user_fname string,
  user_lname string,
  user_email string
) using delta

-- COMMAND ----------

INSERT into crud_demo 
VALUES
  (1, 'Scott', 'Tiger', 'stiger@gmail.com'),
  (2, 'Donald', 'Duck', 'donaldduck@gmail.com')

-- COMMAND ----------

select * from crud_demo

-- COMMAND ----------

INSERT into crud_demo 
VALUES
  (3, 'Mickey', 'Mouse', 'mmouse@gmail.com'),
  (4, 'Tom', 'Jerry', 'tjerry@gmail.com')

-- COMMAND ----------

select * from crud_demo

-- COMMAND ----------

update crud_demo
set user_email = 't.jerry@gmail.com'
where user_id = 4

-- COMMAND ----------

select * from crud_demo

-- COMMAND ----------

create table crud_demo_stg (
  user_id INT, 
  user_fname string,
  user_lname string,
  user_email string
) using delta

-- COMMAND ----------

select * from crud_demo_stg

-- COMMAND ----------

insert into crud_demo_stg 
VALUES
  (3, 'Datta', 'Padal', 'dp@gmail.com'),
  (5, 'Kishan', 'K', 'KK@gmail.com'),
  (6, 'Raja', 'Shakeel', 'Sraja@gmail.com')

-- COMMAND ----------

merge into crud_demo cd
using crud_demo_stg cdg
  on cd.user_id = cdg.user_id 
when matched then update set * 
when not matched then insert * 


-- COMMAND ----------

merge into crud_demo cd
using crud_demo_stg cdg
  on cd.user_id = cdg.user_id 
when matched then update set 
  cd.user_lname = cdg.user_lname,
  cd.user_fname = cdg.user_fname, 
  cd.user_email = cdg.user_email
when not matched then insert 
  (cd.user_id, cd.user_fname, cd.user_lname, cd.user_email)
  Values
  (cdg.user_id, cdg.user_fname, cdg.user_lname, cdg.user_email)


-- COMMAND ----------


