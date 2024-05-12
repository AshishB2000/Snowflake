--- creating a showpipe

--cteating table

create or replace table WORK1_DB.PUBLIC.data_froms3(
index int,
customer varchar(20),
first_name string(20),
last_name string(20),
company string(20),
city string(10),
country string(10),
phone_num1 varchar(20),
phone_num2 varchar(20),
email varchar(30),
subscription_date varchar(20),
website varchar(40)
)


--creating file format

create or replace file format MANAGE_DB.FILE_FORMAT.s3csv_format
type= csv
field_delimiter = ','
skip_header= 1
null_if=('Null','null','NULL')
empty_field_as_null = true;


-- creating stage
 create or replace stage MANAGE_DB.EXTERNAL_STAGES.s3csv
 url= 's3://showflakeashish110/csvfile/pipe'
 storage_integration = s3_bucket
 file_format= MANAGE_DB.FILE_FORMAT.s3csv_format


 list @MANAGE_DB.EXTERNAL_STAGES.s3csv

--creating new schema for pipe
 create or replace schema MANAGE_DB.pipes


 --- crearting a snoe pipe
 
 create pipe MANAGE_DB.pipes.customerpipe
 auto_ingest= true
 as
 copy into WORK1_DB.PUBLIC.data_froms3
 from @MANAGE_DB.EXTERNAL_STAGES.s3csv
 on_error= 'continue'


 -- copy the notification_channel and add it to the event notification in s3 bucket 
 desc pipe  MANAGE_DB.pipes.customerpipe


 select * from WORK1_DB.PUBLIC.data_froms3
