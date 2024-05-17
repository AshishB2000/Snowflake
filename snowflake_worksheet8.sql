---cloning 

select * from COPY_DB.PUBLIC.ORDERS

create table COPY_DB.PUBLIC.ORDERS_clone
clone  COPY_DB.PUBLIC.ORDERS

select * from COPY_DB.PUBLIC.ORDERS

show tables in COPY_DB.PUBLIC


--cloning with timetravel
create table COPY_DB.PUBLIC.ORDERS_clone
clone  COPY_DB.PUBLIC.ORDERS at (offset=>-60*1.5)



-- data sharing

select * from COPY_DB.PUBLIC.ORDERS


-- creating a share object 
create or replace share orders_share

--setup the grants

grant usage on database copy_db to share  orders_share

grant usage on schema COPY_DB.public to share  orders_share

grant select  on table COPY_DB.PUBLIC.ORDERS to share orders_share


show grants on share orders_share;


alter share orders_share add account='consumer account number'


-- on the consumer account
--show shares;
--create database data_s from share <producer_account>.order_share
--slect * from data_s.public.orders



--sampling 

--(1 is 1% of the rows and seed so that other person gets same result if he out the seed = 27)
create or replace view address_sample
as
select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER
sample row(1) seed(27);

select * from address_sample;


--task

--creating a task
create or replace task order_insert
warehouse= compute_wh
schedule='1 minute'
as 
insert into order(date) values(current_date;

-- to start task
alter task order_insert resume;
alter task order_insert suspend;

--using the cron
create or replace task order_insert1
warehouse= compute_wh
schedule='using cron ***** UTC'
as 
insert into order(date) values(current_date); 


--tree of tasks()
create or replace task order_insert
warehouse= compute_wh
after order_insert1
schedule=''1 minute''
as 
insert into order(date) values(current_date;
