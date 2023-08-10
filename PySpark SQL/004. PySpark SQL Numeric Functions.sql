-- Databricks notebook source
select abs(-10.5), abs(10)

-- COMMAND ----------

use database dattadb

-- COMMAND ----------

show tables

-- COMMAND ----------

select order_item_order_id, order_item_subtotal from order_items where order_item_order_id = 2

-- COMMAND ----------

select round(avg(order_item_subtotal),2) from order_items where order_item_order_id = 2

-- COMMAND ----------

select order_item_order_id, round(avg(order_item_subtotal),2) from order_items group by 1  ORDER by 1 limit 10

-- COMMAND ----------

select order_item_order_id, count(order_item_id), round(sum(order_item_subtotal),2) from order_items group by 1  ORDER by 1 limit 10

-- COMMAND ----------

select 
  round(10.58) round_,          -- 11
  round(10.28) round2_,         -- 10
  round(10.28, 1) round3_,      -- 10.3
  round(10.25, 1) round4_,      -- 10.3
  round(10.24, 1) round5_,      -- 10.2
  floor(10.58) floor_,          -- 10
  floor(10.28) floor2_,         -- 10
  ceil(10.58) ceil_,            -- 11
  ceil(10.28) ceil2_            -- 11


-- COMMAND ----------

select greatest(10,11, 13, -1)

-- COMMAND ----------

select greatest(10.5,11, 13, -1)

-- COMMAND ----------

select rand() as rand

-- COMMAND ----------

select cast(rand()*10 as int) as random_int -- generates random number between 0 and 9 

-- COMMAND ----------

select 
  order_item_order_id, 
  count(order_item_id) as order_item_count, 
  round(sum(order_item_subtotal),2)  as order_item_total, 
  min(order_item_subtotal) as order_item_min_total,
  max(order_item_subtotal) as order_item_max_total
from order_items 
group by 1  
ORDER by 1 
limit 10

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Data type conversion (type casting)

-- COMMAND ----------

select curdate()

-- COMMAND ----------

select date_format(curdate(), 'MM') as mth_string

-- COMMAND ----------

desc function cast

-- COMMAND ----------

select 
  date_format(curdate(), 'MM') as mth_string,
  cast(date_format(curdate(), 'MM')  as int) mth_int

-- COMMAND ----------

select 
  cast('0.04' as float),
  cast('4' as float),
  cast( 4 as float),
  cast (4.23457 as int)

-- COMMAND ----------

select cast('xyz' as int) returns_null

-- COMMAND ----------

describe formatted orders

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/tables/orders

-- COMMAND ----------

drop table if exists order_single_column

-- COMMAND ----------

create table order_single_column (
  s string
) using text
options (
  path = 'dbfs:/FileStore/tables/orders'
)

-- COMMAND ----------

select * from order_single_column limit 10

-- COMMAND ----------

select split(s, ',') from order_single_column limit 10

-- COMMAND ----------

select 
  cast(split(s, ',') [0] as int) as order_id,
  cast(split(s, ',')[1] as timestamp) as order_date, 
  cast(split(s, ',')[2] as int) as order_customer_id, 
  cast(split(s, ',')[3] as string)as order_status -- not really needed
from 
  order_single_column 
limit 10

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Handling NULL values

-- COMMAND ----------

select concat('Hello', null)

-- COMMAND ----------

select 1+null

-- COMMAND ----------

 desc function nvl

-- COMMAND ----------

desc function coalesce

-- COMMAND ----------

desc function nvl2

-- COMMAND ----------

select nvl(1, 0) as nvl_, coalesce(1, 0) as coalesce_  -- 1, 1

-- COMMAND ----------

select nvl(null, 0) as nvl_, coalesce(null, 0) as coalesce_  -- 0, 0

-- COMMAND ----------

select coalesce(null, null, 2, null, 3 ) coalesce_ -- 2

-- COMMAND ----------

drop table if exists sales

-- COMMAND ----------

create table sales (
  sales_id int, 
  sales_amount float, 
  commission_pct int
)

-- COMMAND ----------

insert into sales 
values
(1, 1000, 10), 
(2, 1500, 8), 
(3, 500, null), 
(4, 800, 5), 
(5, 250, null)

-- COMMAND ----------

select * from sales order by 1

-- COMMAND ----------

select 
  s.*, 
  nvl(commission_pct, 0) as commission_pct2
from 
  sales s
order by 1


-- COMMAND ----------

select 
  s.*, 
  coalesce(commission_pct, 0) as commission_pct2
from 
  sales s
order by 1

-- COMMAND ----------

select
  s.*, 
  round(sales_amount * commission_pct/100, 2) as incorrect_commission_amount, 
  round(sales_amount * nvl(commission_pct, 0)/100, 2) commission_amount
from 
  sales s
order by 1

-- COMMAND ----------

select
  s.*, 
  round(sales_amount * commission_pct/100, 2) as incorrect_commission_amount, 
  round(sales_amount * coalesce(commission_pct, 0)/100, 2) commission_amount
from 
  sales s
order by 1

-- COMMAND ----------

select
  s.*, 
  round(sales_amount * coalesce(commission_pct, 0)/100, 2) commission_amount, 
  sales_amount - round(sales_amount * coalesce(commission_pct, 0)/100, 2) as revenue
from 
  sales s
order by 1

-- COMMAND ----------

-- add 1% if commission_pct is not null
-- If comission_pct is null, set it to 2%
select
  s.*, 
  nvl2(s.commission_pct, s.commission_pct+1, 2) as mdfd_commission_pct
from 
  sales s
order by 1

-- COMMAND ----------

select
  s.*, 
  nvl2(s.commission_pct, s.commission_pct+1, 2) as mdfd_commission_pct, 
  round(s.sales_amount * nvl2(s.commission_pct, s.commission_pct+1, 2)/100, 2 ) as mdfd_commission_amount
from 
  sales s
order by 1

-- COMMAND ----------

select
  s.*, 
  case when s.commission_pct is null then 2
       else s.commission_pct + 1
       end
  as mdfd_commission_pct
  
from 
  sales s
order by 1

-- COMMAND ----------

describe orders

-- COMMAND ----------

select distinct order_status from orders

-- COMMAND ----------

select 
  distinct order_status,
  case 
    when order_status in ('COMPLETE', 'CLOSED') then 'COMPLETED'
    when order_status in ('PENDING', 'PENDING_PAYMENT', 'PROCESSING', 'PAYMENT_REVIEW') then 'PENDING'
    else 'OTHER'
  end as updated_order_status 
from orders

-- COMMAND ----------

select 
  order_status, 
  count(*) as order_count
from 
  orders 
group by 
  order_status
order by 
  order_status


-- COMMAND ----------

select 
  case 
    when order_status in ('COMPLETE', 'CLOSED') then 'COMPLETED'
    when order_status in ('PENDING', 'PENDING_PAYMENT', 'PROCESSING', 'PAYMENT_REVIEW') then 'PENDING'
    else 'OTHER'
  end as updated_order_status, 
  count(*) as order_count
from 
  orders 
group by 
  1
order by 
  1

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Word count example

-- COMMAND ----------

create table lines (s string)


-- COMMAND ----------

insert into lines 
values 
('Hello World'),
('How are you'),
('Let us perform the word count'), 
('The definition of word count is'),
('to get the count of each word from this data')

-- COMMAND ----------

select * from lines

-- COMMAND ----------

select split(s, ' ') as word_array from lines

-- COMMAND ----------

select explode(split(s, ' ')) as words from lines

-- COMMAND ----------

-- explode cannot be used in group by, it will give you incorrect values

--select explode(split(s, ' ')) as words, count(*) from lines group by explode(split(s, ' ')) order by 1

-- Error in SQL statement: AnalysisException: [UNSUPPORTED_GENERATOR.OUTSIDE_SELECT] The generator is not supported: outside the SELECT clause, found: Aggregate [explode(split(s#5857,  , -1))], [split(s#5857,  , -1) AS _gen_input_0#5860, count(1) AS count(1)#5859L].

-- COMMAND ----------

with exploded_words as (
  select explode(split(s, ' ')) as word from lines
)

Select word, count(*) from exploded_words group by word order by word

-- COMMAND ----------


