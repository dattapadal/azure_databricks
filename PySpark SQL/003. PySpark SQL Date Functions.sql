-- Databricks notebook source
-- Date : yyyy-MM-dd
-- Timestamp : yyyy-MM-ddTHH:MM:ss.SSS
select current_date() as current_date

-- COMMAND ----------

select current_date as current_date

-- COMMAND ----------

select current_timestamp as current_timestamp

-- COMMAND ----------

select current_date as current_date, date_add(current_date, 15) as date_after_addition

-- COMMAND ----------

select current_date as current_date, date_add(current_date, 730) as date_after_addition

-- COMMAND ----------

select current_date as current_date, date_add(current_date, -10) as date_after_subtraction

-- COMMAND ----------

select current_date as current_date, date_sub(current_date, 10) as date_after_subtraction

-- COMMAND ----------

select date_diff('2023-08-09', '2023-07-30') as difference

-- COMMAND ----------

select date_diff( '2023-07-30', '2023-08-09') as difference

-- COMMAND ----------

select add_months(current_date(), 4) as after_adding_mths

-- COMMAND ----------

select add_months('2019-01-31', 1) as after_adding_mths

-- COMMAND ----------

select add_months('2019-02-29', 1) as after_adding_mths -- result will be null

-- COMMAND ----------

desc function trunc

-- COMMAND ----------

select trunc(current_date(), 'MM') as beginning_date_of_month

-- COMMAND ----------

select trunc('2023-08-23', 'MM') as beginning_date_of_month

-- COMMAND ----------

select trunc(current_date(), 'yy') as beginning_date_of_year

-- COMMAND ----------

select trunc('2023-12-01', 'yy') as beginning_date_of_year

-- COMMAND ----------

select trunc(current_timestamp, 'yy') as beginning_date_of_year

-- COMMAND ----------

select trunc(current_timestamp, 'HH') as beginning_hour -- wont work as with trunc we can only get month and year truncated.

-- COMMAND ----------

select current_timestamp, date_trunc('HOUR', current_timestamp()) as hour_beginning

-- COMMAND ----------

select current_timestamp, date_trunc('MINUTE', current_timestamp()) as min_beginning

-- COMMAND ----------

select current_timestamp, date_trunc('DAY', current_timestamp()) as day_beginning

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extracting information using date_format
-- MAGIC

-- COMMAND ----------

desc function date_format

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'yyyy') as year, date_format(current_timestamp(), 'yy') as year_2

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'MM') as month

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'dd') as day

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'DDD') as day_of_year

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'MMM') as month_name3 , date_format(current_timestamp(), 'MMMM') as month_name_full 

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'EE') as week_name2 , date_format(current_timestamp(), 'EEEE') as week_name_full 

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'HH') as hour24 , date_format(current_timestamp(), 'hh') as hour12 

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'mm') as minutes 

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'ss') as seconds 

-- COMMAND ----------

select current_timestamp(), date_format(current_timestamp(), 'SSS') as milli_seconds 

-- COMMAND ----------

select date_format(current_timestamp(), 'yyyyMM') as YearMonth

-- COMMAND ----------

select date_format(current_timestamp(), 'yyyyMMdd') as YearMonthdate

-- COMMAND ----------

select date_format(current_timestamp(), 'yyyyDDD') as YearDays

-- COMMAND ----------

select date_format(current_timestamp(), 'dd MMMM, yyyy') 

-- COMMAND ----------

select date_format(current_timestamp(), 'yyyy/MM/dd') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managing Non-standard Dates and Timestamps

-- COMMAND ----------

select to_date('2022/1/16', 'yyyy/M/dd') as result

-- COMMAND ----------

select to_timestamp('2022/1/16 18:24', 'yyyy/M/d HH:mm') as result

-- COMMAND ----------

select date_format(to_timestamp('2022/1/16 18:24', 'yyyy/M/d HH:mm'), 'HH') as hour24, date_format(to_timestamp('2022/1/16 18:24', 'yyyy/M/d HH:mm'), 'hh') as hour12

-- COMMAND ----------

select to_date(20221015, 'yyyyMMdd') as result

-- COMMAND ----------

select to_date(2023100, 'yyyyDDD') as result

-- COMMAND ----------

select trunc(to_date(2023100, 'yyyyDDD'), 'MM') as result

-- COMMAND ----------

select date_format(to_date(2023100, 'yyyyDDD'), 'EE') as result

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Extracting information - calendar functions
-- MAGIC

-- COMMAND ----------

desc function day

-- COMMAND ----------

desc function dayofmonth

-- COMMAND ----------

desc function month

-- COMMAND ----------

desc function weekofyear

-- COMMAND ----------

desc function year

-- COMMAND ----------

select weekofyear(current_date())

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ###Dealing with Unix Timestamp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC !date '+%s'

-- COMMAND ----------

desc function from_unixtime

-- COMMAND ----------

desc function unix_timestamp

-- COMMAND ----------

desc function to_unix_timestamp

-- COMMAND ----------

select from_unixtime(1667779999) as timestamp

-- COMMAND ----------

select from_unixtime(1667779999, 'yyyy-MM') as timestamp

-- COMMAND ----------

select to_unix_timestamp('2023-08-09 20:00:15')

-- COMMAND ----------

select unix_timestamp()

-- COMMAND ----------


