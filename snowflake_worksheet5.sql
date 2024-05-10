--Loading s3 bucket data



-- Ccreating a integration object
create or replace storage integration s3_bucket
type= external_stage
storage_provider= s3
enabled= true
storage_aws_role_arn = '-----'
storage_allowed_locations=('s3://showflakeashish110/csvfile' , 's3://showflakeashish110/jsondata') -- can porovide multiple buckets
 comment='<comment>'

 desc integration s3_bucket




-- creating a table


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


create or replace file format MANAGE_DB.FILE_FORMAT.s3csv_format
type= csv
field_delimiter = ','
skip_header= 1
null_if=('Null','null','NULL')
empty_field_as_null = true;



 -- creating a stage by the integration object 


 create or replace stage MANAGE_DB.EXTERNAL_STAGES.s3csv
 url= 's3://showflakeashish110/csvfile'
 storage_integration = s3_bucket
 file_format= MANAGE_DB.FILE_FORMAT.s3csv_format


 copy into WORK1_DB.PUBLIC.data_froms3
 from @MANAGE_DB.EXTERNAL_STAGES.s3csv
 
