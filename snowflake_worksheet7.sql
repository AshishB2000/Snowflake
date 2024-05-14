-- time travel
-- if we did some mistake on the table and want to go back we can do

-- going back 1.5min(we go back to 90days for enterpise and 1day for standard)
select * from COPY_DB.PUBLIC.ORDERS at (offset=>-60*1.5)

--or we can use the timestamp
select * from COPY_DB.PUBLIC.ORDERS before(timestamp=> '2024-05-13 16:45:11.686'::timestamp)


--or we can just use the query_id

select * from COPY_DB.PUBLIC.ORDERS before (statement =>'01b44f91-0305-2294-0005-b5d30002d18e' )

-- we can not insert the backup data to the same table where mistake happend(ORDERS) we have to create new table ORDERS.backup like 

create or replace table COPY_DB.PUBLIC.ORDERS_backup as 
select * from COPY_DB.PUBLIC.ORDERS before (statement =>'01b44f91-0305-2294-0005-b5d30002d18e' )
-- and then truncatr the order main table and insert the backup to main table like 

truncate COPY_DB.PUBLIC.ORDERS

insert into COPY_DB.PUBLIC.ORDERS
select * from COPY_DB.PUBLIC.ORDERS_backup




--undropping (UNDROP)
drop table COPY_DB.PUBLIC.ORDERS

--if we dropped table by mistake we can undrop the table
undrop table COPY_DB.PUBLIC.ORDERS

-- we can do same the undropping with table,database, schema



-- we can restore the replaced tables too
create or replace table COPY_DB.PUBLIC.ORDERS_backup as 
select * from COPY_DB.PUBLIC.ORDERS before (statement =>'01b44f91-0305-2294-0005-b5d30002d18e' )

-- so here we can get the main ORDERS_backup table before being replaced
undrop table COPY_DB.PUBLIC.ORDERS_backup;
alter table COPY_DB.PUBLIC.ORDERS_backup
rename to COPY_DB.PUBLIC.ORDERS_backup_2;