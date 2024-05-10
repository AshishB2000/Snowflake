SELECT * FROM SNOWFLAKE_SAMPLE_DATA.
+TPCH_SF1.CUSTOMER



--CREATE WAREHOUSE

CREATE OR REPLACE WAREHOUSE COMPUTE_WH1
WITH 
WAREHOUSE_SIZE=XSMALL
MAX_CLUSTER_COUNT=3
AUTO_SUSPEND= 300
AUTO_RESUME = TRUE
COMMENT = 'THIS IS WHAREHOUSE CREATED BY THE SQL'


-- DROP THE WAREHOUSE 

DROP WAREHOUSE 



-- CREATING A DATABASE AND A TABLE AND LOADING DATA FROM S3 USING URL

CREATE OR REPLACE DATABASE WORK1_DB;

CREATE OR REPLACE TABLE CUSTOMERS(
customer_id int,
first_name varchar,
last_name varchar,
email varchar,
age int,
city varchar
)


COPY INTO CUSTOMERS
FROM s3://snowflake-assignments-mc/gettingstarted/customers.csv
FILE_FORMAT=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1
);

SELECT * FROM CUSTOMERS



-- creating a stage

create or replace database manage_db

create or replace schema external_stages;

//creating external stage (use the storage integration object instead of putting directly the aws key and secret key)
create or replace stage manage_db.external_stages.aws_stage
url='s3://bucketsnowflakes3'
credentials=(aws_key_id='ABCD_DUMMY_ID'aws_secret_key='1234abcd_key');



    
desc stage aws_stage;


// for altering the stage

alter stage aws_stage
set credentials=(aws_key_id='.....'aws_secret_key='.....')


create or replace stage manage_db.external_stages.aws_stage
url='s3://bucketsnowflakes3'

// list the stage use @ befor the stage 

list @aws_stage;

--Loading the data from external stage to snowflake
create or replace table work1_db.public.orders(
order_id varchar(20),
amount int,
profit int,
quantity int,
category varchar(20),
subcategory varchar(20)
);

select * from  work1_db.public.orders;

//error because we have not selected which file in stage 
copy into work1_db.public.orders
from @aws_stage 
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1);
)


list @aws_stage;

copy into work1_db.public.orders
from @aws_stage 
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv'); // or pattern =('*Order.*csv')

create or replace table WORK1_DB.PUBLIC.order_ox(
order_id varchar(10),
amount int
)

--Transfoemation using the select from stage to table 

copy into WORK1_DB.PUBLIC.order_ox
from (select s.$1, s.$2 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s)
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv');


select * from WORK1_DB.PUBLIC.order_ox


--Transfoemation using the select from stage to table using SQL functions 

copy into WORK1_DB.PUBLIC.order_quan 
from (
select s.$1,s.$2,s.$4, case when s.$4 > 2 then 'bulk quantitiy' else 'not bulk' end from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv');

select * from WORK1_DB.PUBLIC.order_quan 

--Transfoemation using the select from stage to table using  subset of columns
--slecting only 2 columns 

create or replace table WORK1_DB.PUBLIC.order_ox(
order_id varchar(10),
amount int,
profit int,
order_profit varchar(20)
)


copy into WORK1_DB.PUBLIC.order_ox(order_id,amount)
from (
select s.$1,s.$2 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv');


select * from WORK1_DB.PUBLIC.order_ox


-- Auto increment id 

create or replace table WORK1_DB.PUBLIC.order_ox(
order_id int autoincrement start 1000 increment 1,
amount int
)


copy into WORK1_DB.PUBLIC.order_ox(amount)
from (
select s.$2 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv');

select * from WORK1_DB.PUBLIC.order_ox


--remove all data from the table 

truncate table WORK1_DB.PUBLIC.order_ox

-- Dealing with errors



create or replace table WORK1_DB.PUBLIC.order_ox(
order_id int autoincrement start 1000 increment 1,
amount int
)

-- on error option 
copy into WORK1_DB.PUBLIC.order_ox(amount)
from (
select s.$1 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv')
on_error = 'continue';

select * from WORK1_DB.PUBLIC.order_ox

-- using on_error option (abort_statement) which is the default one 

copy into WORK1_DB.PUBLIC.order_ox(amount)
from (
select s.$1 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv')
on_error = 'abort_statement';


-- using the skip file (so here it skips the OrderDetails_with error.csv file )

copy into WORK1_DB.PUBLIC.order_ox(amount)
from (
select s.$1 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1)
files=('OrderDetails.csv', 'OrderDetails_with error.csv')
on_error = 'skip_file';





-- using the file format 


create or replace table WORK1_DB.PUBLIC.order_ox(
order_id varchar(10),
amount int,
profit int,
order_profit varchar(20)
);

create or replace schema MANAGE_DB.file_format;

create or replace file format MANAGE_DB.file_format.my_fileformat;


-- describing the file format(which is default to csv)
desc file format MANAGE_DB.file_format.my_fileformat;




create or replace table WORK1_DB.PUBLIC.order_ox(
order_id varchar(10),
amount int,
profit int,
order_profit varchar(20)
)


copy into WORK1_DB.PUBLIC.order_ox(order_id,amount)
from (
select s.$1,s.$2 from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s )
file_format=(
format_name = MANAGE_DB.file_format.my_fileformat
)
files=('OrderDetails.csv');


--Atering the file format "we can not alter the type of the file format we can only alter the other properties"

alter file format MANAGE_DB.file_format.my_fileformat   
set skip_header = 1;

--creating a new file format using putting properties 

create or replace file format MANAGE_DB.FILE_FORMAT.my_fileformat 
TYPE=CSV
FIELD_DELIMITER= ','
SKIP_HEADER= 1;

create or replace table WORK1_DB.PUBLIC.order_ox(
order_id varchar(10),
amount int
)


copy into WORK1_DB.PUBLIC.order_ox
from (select s.$1, s.$2, from @MANAGE_DB.EXTERNAL_STAGES.AWS_STAGE s)
file_format=(
format_name = MANAGE_DB.FILE_FORMAT.my_fileformat)
files=('OrderDetails.csv');


