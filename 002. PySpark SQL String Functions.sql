-- Databricks notebook source
show functions

-- COMMAND ----------

desc function substr

-- COMMAND ----------

select substr('Hello World', 1, 5)

-- COMMAND ----------

select lower("HeLLO WORlD" ), upper("HeLLO WORlD"), initcap("hellO wORLD")

-- COMMAND ----------

select length("Hello World")

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use database dattadb

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from orders limit 10

-- COMMAND ----------

select order_id, order_date, order_customer_id, lower(order_status) as order_status, length(order_status) as order_status_length
from orders

-- COMMAND ----------

desc function substr

-- COMMAND ----------

-- extracting last 4 digits of SSN 
select substr('123456789', -4)

-- COMMAND ----------

select substr('123456789', 4)

-- COMMAND ----------

select substr('2023-01-25 00:00:00.0', 1, 10) as order_date

-- COMMAND ----------

select 
  order_id, 
  substr(order_date, 1, 10) as order_date, 
  order_customer_id, 
  lower(order_status) as order_status 
from orders 


-- COMMAND ----------

select distinct substr(order_date, 1, 10) as order_date from orders order by 1 

-- COMMAND ----------

desc function split

-- COMMAND ----------

-- split converts delimited string into array of values with index starting at zero.
select split('1234567890, 1234567891', ',') as phone_numbers

-- COMMAND ----------

select split('1234567890, 1234567891', ',')[1] as phone_numbers

-- COMMAND ----------

desc function explode

-- COMMAND ----------

select explode(split('1234567890, 1234567891', ',')) as phone_numbers

-- COMMAND ----------

-- ltrim - removes spaces from left
select ltrim('              Datta Padal               ') as ltrimmed

-- COMMAND ----------

-- rtrim - removes spaces from right
select rtrim('              Datta Padal               ') as ltrimmed

-- COMMAND ----------

--trim - removes spaces from both siders
select trim('              Datta Padal               ') as ltrimmed

-- COMMAND ----------

desc function trim

-- COMMAND ----------

select trim(LEADING FROM '              Datta Padal               ') as result

-- COMMAND ----------

select trim(TRAILING FROM '              Datta Padal               ') as result

-- COMMAND ----------

select trim(BOTH FROM '              Datta Padal               ') as result

-- COMMAND ----------

select trim(LEADING 'A' FROM 'AAAAAAAAAAAAAAAAAAADatta PadalBBBBBBBBBBBBBBBBB') as result

-- COMMAND ----------

select trim(TRAILING 'B' FROM 'AAAAAAAAAAAAAAAAAAADatta PadalBBBBBBBBBBBBBBBBB') as result

-- COMMAND ----------

select trim(TRAILING 'B' from trim(LEADING 'A' FROM 'AAAAAAAAAAAAAAAAAAADatta PadalBBBBBBBBBBBBBBBBB')) as result

-- COMMAND ----------

select trim('AB' FROM 'AAAAAAAAAAAAAAAAAAADatta PadalBBBBBBBBBBBBBBBBB') as result

-- COMMAND ----------

select trim('AB' FROM 'ABABABABABADatta PadalABABABABB') as result

-- COMMAND ----------

select trim('AB' FROM 'ABABABABABAAAAAAAAAAAAAAAADatta PadalABABABABBBBBBBBBBBBBBBBBBB') as result

-- COMMAND ----------

select trim('AB' FROM 'ABABABABABAAAAAAAAAAAAAAAADatta AB PadalABABABABBBBBBBBBBBBBBBBBBB') as result

-- COMMAND ----------

Select 2023 as year_, 01 as month_, 25 as date_

-- COMMAND ----------

desc function lpad

-- COMMAND ----------

select lpad(7, 2, 0) as result

-- COMMAND ----------

select lpad(10, 2, 0) as result

-- COMMAND ----------

select lpad(1984, 2, 0) as result

-- COMMAND ----------

use dattadb

-- COMMAND ----------

create table sales_fact (
  sale_year int,
  sale_month int, 
  sale_day int, 
  order_revenue float, 
  order_count int
)

-- COMMAND ----------

desc table sales_fact

-- COMMAND ----------

insert into sales_fact
values 
  (2012, 1, 1, 1000.00, 3), 
  (2012, 1, 10, 1250.00, 2), 
  (2012, 2, 5, 1300.00, 5)
  

-- COMMAND ----------

select * from sales_fact

-- COMMAND ----------

select 
  sale_year, 
  lpad(sale_month, 2, 0) as sale_month, 
  lpad(sale_day, 2, 0) as sale_day, 
  order_revenue, 
  order_count
from sales_fact

-- COMMAND ----------

select 
  to_date(concat_ws('-', sale_year, lpad(sale_month, 2, 0), lpad(sale_day, 2, 0))) as sale_date,
  order_revenue, 
  order_count
from sales_fact

-- COMMAND ----------

drop table sales_fact

-- COMMAND ----------

select reverse('Datta Padal')

-- COMMAND ----------

select concat('Datta', 'Padal')

-- COMMAND ----------

select concat('Datta ', 'Padal')

-- COMMAND ----------

select concat('Hello', 'World', 'How' , 'are', 'you')

-- COMMAND ----------

select concat_ws('Hello', 'World', 'How' , 'are', 'you')

-- COMMAND ----------

select concat_ws(' ', 'Hello', 'World', 'How' , 'are', 'you')

-- COMMAND ----------

select concat_ws('_', 'Hello', 'World', 'How' , 'are', 'you')

-- COMMAND ----------

select * from orders

-- COMMAND ----------

Select 
  order_id, 
  order_date, 
  order_customer_id, 
  concat('Order is ', lower(order_status)) as order_status 
from orders 

-- COMMAND ----------

Select 
 Distinct
  concat('Order is ', lower(order_status)) as order_status 
from orders 

-- COMMAND ----------

desc function concat_Ws

-- COMMAND ----------

Select array('1234567890', '1234567891', '1234567892') as phone_numbers

-- COMMAND ----------

Select concat_ws(';', array('1234567890', '1234567891', '1234567892')) as phone_numbers

-- COMMAND ----------

 
