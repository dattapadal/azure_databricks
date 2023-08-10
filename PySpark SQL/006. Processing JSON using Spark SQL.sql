-- Databricks notebook source
use database dattadb

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables/

-- COMMAND ----------

drop table if exists users

-- COMMAND ----------

create table users (
  user_id int, 
  user_fname string, 
  user_lname string, 
  user_phones array<string>
)

-- COMMAND ----------

insert into users 
values 
  (1, 'Scott', 'Tiger', array('+1 (234) 567 8901', '+1 (123) 456 7890')),
  (2, 'Donald', 'Duck', null),
  (3, 'Mickey', 'Mouse', array('+1 (456) 789 0123'))

-- COMMAND ----------

select * from users 

-- COMMAND ----------

select user_id, size(user_phones) phone_count from users 

-- COMMAND ----------

select user_id, nvl2(user_phones, size(user_phones),0)  as phone_count from users 

-- COMMAND ----------

select user_id, explode(user_phones) user_phone from users 

-- COMMAND ----------

select user_id, explode_outer(user_phones) user_phone from users 

-- COMMAND ----------

drop table if exists users

-- COMMAND ----------

create table users (
  user_id int, 
  user_fname string, 
  user_lname string, 
  user_phones struct<home:string, mobile:string>
)

-- COMMAND ----------

insert into users 
values 
  (1, 'Scott', 'Tiger', struct('+1 (234) 567 8901', '+1 (123) 456 7890')),
  (2, 'Donald', 'Duck', struct(null, null)),
  (3, 'Mickey', 'Mouse', struct('+1 (456) 789 0123', null))

-- COMMAND ----------

select * from users

-- COMMAND ----------

select 
  user_id, user_phones.*
from users

-- COMMAND ----------

select 
  user_id, user_phones.home, user_phones.mobile
from users

-- COMMAND ----------

select 
  order_item_id, 
  order_item_order_id,
  order_item_product_id,
  order_item_subtotal,
  struct(order_item_quantity, order_item_product_price) as order_item_trans_details
from order_items limit 10

-- COMMAND ----------

drop table if exists users

-- COMMAND ----------

create table users (
  user_id int, 
  user_fname string, 
  user_lname string, 
  user_phones ARRAY<STRUCT<phone_type: string, phone_number: string>>
)

-- COMMAND ----------

insert into users 
values 
  (1, 'Scott', 'Tiger', array(struct('home', '+1 (234) 567 8901'), struct('mobile', '+1 (123) 456 7890') )),
  (2, 'Donald', 'Duck', null),
  (3, 'Mickey', 'Mouse', array(struct('home', '+1 (123) 456 9012') ))

-- COMMAND ----------

select * from users

-- COMMAND ----------

select
  user_id,
  explode_outer(user_phones)
from 
  users

-- COMMAND ----------

with user_phones_exploded as (
  select
  user_id,
  explode_outer(user_phones) as user_phones
from 
  users
)

select user_id, user_phones.* from user_phones_exploded

-- COMMAND ----------

 with user_phones_exploded as ( 
  select
  user_id,
  explode_outer(user_phones) as user_phones
from 
  users
)

select user_id, user_phones.phone_type, user_phones.phone_number from user_phones_exploded

-- COMMAND ----------

drop table if exists users

-- COMMAND ----------

create table users (
  user_id int, 
  user_fname string, 
  user_lname string, 
  user_phone_type string,
  user_phone_number string
)

-- COMMAND ----------

insert into users 
values 
  (1, 'Scott', 'Tiger', 'home', '+1 (234) 567 8901'),
  (1, 'Scott', 'Tiger', 'mobile', '+1 (123) 456 7890' ),
  (2, 'Donald', 'Duck', null, null),
  (3, 'Mickey', 'Mouse', 'home', '+1 (123) 456 9012')

-- COMMAND ----------

select * from users

-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  collect_list(user_phone_number) as user_phones
from users
group by all
order by 1, 2, 3

-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  concat_ws(',', collect_list(user_phone_number)) as user_phones
from users
group by all
order by 1, 2, 3

-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  nvl2(user_phone_number, struct(user_phone_type, user_phone_number), null) as user_phones
from users


-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  collect_list(nvl2(user_phone_number, struct(user_phone_type, user_phone_number), null)) as user_phones
from users
group by all
order by 1, 2, 3

-- COMMAND ----------

drop table if exists users

-- COMMAND ----------

create table users (
  user_id int, 
  user_fname string, 
  user_lname string, 
  user_phones string
)

-- COMMAND ----------

insert into users 
values 
  (1, 'Scott', 'Tiger', '+1 (234) 567 8901, +1 (123) 456 7890'),
  (2, 'Donald', 'Duck', null),
  (3, 'Mickey', 'Mouse', '+1 (123) 456 9012')

-- COMMAND ----------

select * from users

-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  split(user_phones, ',') 
 from users

-- COMMAND ----------

select 
  user_id,
  user_fname,
  user_lname,
  explode_outer(split(user_phones, ',')) as user_phone
 from users
