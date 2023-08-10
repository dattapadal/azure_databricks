-- Databricks notebook source
use database dattadb

-- COMMAND ----------

describe formatted orders

-- COMMAND ----------

-- MAGIC %fs ls "dbfs:/FileStore/tables/retail_db/orders"

-- COMMAND ----------

-- creating views 
create or replace temporary view orders_v (
  order_id int, 
  order_date date, 
  order_customer_id int, 
  order_status string
) using csv 
options (
  path = 'dbfs:/FileStore/tables/retail_db/orders'
)

-- COMMAND ----------

describe formatted orders_v

-- COMMAND ----------

select * from orders_v limit 10

-- COMMAND ----------

create or replace temporary view order_items_v (
  order_item_id int, 
  order_item_order_id int, 
  order_item_product_id int, 
  order_item_quantity int, 
  order_item_subtotal float,
  order_item_product_price float
) using csv
options (
  path = 'dbfs:/FileStore/tables/retail_db/order_items'
)

-- COMMAND ----------

select * from order_items_v limit 10

-- COMMAND ----------

-- we can copy data from view to the db using
-- insert into <table> select * from <view> 

-- COMMAND ----------

show views

-- COMMAND ----------

show tables -- shows temporary views as well

-- COMMAND ----------

select '' != null , '' is null, null == null, null != null,  null is null, null is not null

-- COMMAND ----------

select * from orders 
order by order_date nulls last -- make sures that if there are any nulls present in the order_date they will be pushed to last in sorting
limit 10

-- COMMAND ----------

select * from orders limit 10

-- COMMAND ----------

select * from order_items limit 10

-- COMMAND ----------

-- computing daily revenue 
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED')
group by
  o.order_date
order by
  o.order_date


-- COMMAND ----------

-- CTAS 
create table if not exists daily_revenue 
as 
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED')
group by
  o.order_date

-- COMMAND ----------

select * from daily_revenue order by 1 limit 10

-- COMMAND ----------

show tables

-- COMMAND ----------

desc formatted daily_revenue

-- COMMAND ----------

show create table daily_revenue

-- COMMAND ----------

drop table daily_revenue

-- COMMAND ----------

CREATE TABLE daily_revenue (
  order_date STRING,
  revenue float)

-- COMMAND ----------

insert into daily_revenue
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED')
group by
  o.order_date

-- COMMAND ----------

select * from daily_revenue

-- COMMAND ----------

insert into daily_revenue
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED')
group by
  o.order_date

-- COMMAND ----------

select count(*) from daily_revenue

-- COMMAND ----------

insert overwrite daily_revenue
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED')
group by
  o.order_date

-- COMMAND ----------

select count(*) from daily_revenue

-- COMMAND ----------

create table daily_revenue_stg (
  order_date date, 
  order_revenue float
)

-- COMMAND ----------

select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED') and
  date_format(o.order_date, 'yyyyMM') = 201307
group by
  o.order_date
order by 
  o.order_date

-- COMMAND ----------

insert overwrite daily_revenue_stg 
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED') and
  date_format(o.order_date, 'yyyyMM') = 201307
group by
  o.order_date

-- COMMAND ----------

select * from daily_revenue_stg

-- COMMAND ----------

truncate table daily_revenue

-- COMMAND ----------

insert into daily_revenue 
select * from daily_revenue_stg 
order by 1

-- COMMAND ----------

select * from daily_revenue

-- COMMAND ----------

-- end to end query pipeline 
create table if not exists daily_revenue_stg (
  order_date date, 
  order_revenue float
);

insert overwrite daily_revenue_stg 
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED') and
  date_format(o.order_date, 'yyyyMM') = 201309
group by
  o.order_date;

insert into daily_revenue 
select * from daily_revenue_stg 
order by 1;

-- COMMAND ----------

select * from daily_revenue

-- COMMAND ----------

-- end to end query pipeline 
drop table if exists daily_revenue_stg;

create table daily_revenue_stg 
as
select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id 
where 
  o.order_status in ('COMPLETE', 'CLOSED') and
  date_format(o.order_date, 'yyyyMM') = 201309
group by
  o.order_date;

insert into daily_revenue 
select * from daily_revenue_stg 
order by 1;

-- COMMAND ----------

truncate table daily_revenue;
truncate table daily_revenue_stg;

-- COMMAND ----------

-- using merge into 
merge into daily_revenue as t 
using (
  select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
  from 
    orders o join order_items oi on o.order_id = oi.order_item_order_id 
  where 
    o.order_status in ('COMPLETE', 'CLOSED') and
    date_format(o.order_date, 'yyyyMM') = 201307
  group by
    o.order_date
) as s 
on t.order_date = s.order_date 
when matched then update set * 
when not matched then insert * 


-- COMMAND ----------

select * from daily_revenue 
order by order_date

-- COMMAND ----------

merge into daily_revenue as t 
using (
  select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
  from 
    orders o join order_items oi on o.order_id = oi.order_item_order_id 
  where 
    o.order_status in ('COMPLETE', 'CLOSED') and
    date_format(o.order_date, 'yyyyMM') = 201307
  group by
    o.order_date
) as s 
on t.order_date = s.order_date 
when matched then update set * 
when not matched then insert * 

-- COMMAND ----------

merge into daily_revenue as t 
using (
  select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
  from 
    orders o join order_items oi on o.order_id = oi.order_item_order_id 
  where 
    o.order_status in ('COMPLETE', 'CLOSED') and
    date_format(o.order_date, 'yyyyMM') = 201308
  group by
    o.order_date
) as s 
on t.order_date = s.order_date 
when matched then update set * 
when not matched then insert * 

-- COMMAND ----------

merge into daily_revenue as t 
using (
  select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
  from 
    orders o join order_items oi on o.order_id = oi.order_item_order_id 
  where 
    o.order_status in ('COMPLETE', 'CLOSED') and
    o.order_date between '2013-07-01' and '2013-09-30'
  group by
    o.order_date
) as s 
on t.order_date = s.order_date 
when matched then update set * 
when not matched then insert * 

-- COMMAND ----------

select * from daily_revenue order by order_date desc

-- COMMAND ----------

merge into daily_revenue as t 
using (
  select 
  o.order_date, round(sum(oi.order_item_subtotal), 2) as revenue
  from 
    orders o join order_items oi on o.order_id = oi.order_item_order_id 
  where 
    o.order_status in ('COMPLETE', 'CLOSED')
  group by
    o.order_date
) as s 
on t.order_date = s.order_date 
when matched then update set * 
when not matched then insert * 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Window functions in Spark SQL
-- MAGIC

-- COMMAND ----------

select * from order_items limit 10

-- COMMAND ----------

select * from orders limit 10

-- COMMAND ----------

select 
  o.order_date , oi.order_item_product_id, sum(oi.order_item_subtotal)
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id
group by o.order_date , oi.order_item_product_id
order by 1, 2

-- COMMAND ----------

select 
  o.order_date , oi.order_item_product_id, round(sum(oi.order_item_subtotal),2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id
group by all
order by 1, 3 desc

-- COMMAND ----------

create temporary view daily_product_revenue as (
  select 
  o.order_date , oi.order_item_product_id, round(sum(oi.order_item_subtotal),2) as revenue
from 
  orders o join order_items oi on o.order_id = oi.order_item_order_id
group by all
)

-- COMMAND ----------

select * from daily_product_revenue

-- COMMAND ----------

select 
  dpr.*, 
  rank() over (order by revenue desc) as rnk,
  dense_rank() over (order by revenue desc) as d_rnk
from daily_product_revenue dpr
where 
  dpr.order_date = '2013-07-25 00:00:00.0' or  dpr.order_date = '2013-07-26 00:00:00.0'

-- COMMAND ----------

select 
  dpr.*, 
  rank() over (partition by order_date order by revenue desc) as rnk,
  dense_rank() over (partition by order_date order by revenue desc) as d_rnk
from daily_product_revenue dpr
where 
  dpr.order_date = '2013-07-25 00:00:00.0' or  dpr.order_date = '2013-07-26 00:00:00.0'

-- COMMAND ----------

with dpr_ranked as (
  select 
  dpr.*, 
  rank() over (partition by order_date order by revenue desc) as rnk,
  dense_rank() over (partition by order_date order by revenue desc) as d_rnk
from daily_product_revenue dpr
where 
  dpr.order_date = '2013-07-25 00:00:00.0' or  dpr.order_date = '2013-07-26 00:00:00.0'
)

select * from dpr_ranked where rnk = 1

-- COMMAND ----------


