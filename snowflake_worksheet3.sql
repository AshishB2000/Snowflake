-- Loading Raw JSON

CREATE OR REPLACE stage MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
     url='s3://bucketsnowflake-jsondemo';

CREATE OR REPLACE file format MANAGE_DB.FILE_FORMAT.MYJSONFORMAT
    TYPE = JSON;
    
    
CREATE OR REPLACE table WORK1_DB.PUBLIC.JSON_RAW (
    raw_file variant);
    
COPY INTO WORK1_DB.PUBLIC.JSON_RAW
    FROM @MANAGE_DB.EXTERNAL_STAGES.JSONSTAGE
    file_format= MANAGE_DB.FILE_FORMAT.MYJSONFORMAT
    files = ('HR_data.json');
    
   
SELECT * FROM WORK1_DB.PUBLIC.JSON_RAW;


--selectinf the columns from json 

SELECT RAW_FILE:city FROM WORK1_DB.PUBLIC.JSON_RAW;

--or
SELECT $1:first_name FROM WORK1_DB.PUBLIC.JSON_RAW;

SELECT 
RAW_FILE:city::String as city_name,
RAW_FILE:id::int as id,
FROM WORK1_DB.PUBLIC.JSON_RAW;


SELECT RAW_FILE:job::String as job FROM WORK1_DB.PUBLIC.JSON_RAW;


-- for the nested data

SELECT RAW_FILE:job.salary::int as salary FROM WORK1_DB.PUBLIC.JSON_RAW;


-- array data
SELECT RAW_FILE:prev_company FROM WORK1_DB.PUBLIC.JSON_RAW;

SELECT RAW_FILE:prev_company[1] FROM WORK1_DB.PUBLIC.JSON_RAW;


SELECT RAW_FILE:spoken_languages[0].level FROM WORK1_DB.PUBLIC.JSON_RAW;

-- using flatten the hierarchy


select 
f.value:language::String as Firat_lang
from WORK1_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) as f;



--creating table from this 

create or replace table spoken_lang as
select
      f.value:language::String as Firat_lang,
      f.value:level::String as level_lang
from WORK1_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) as f;


select * from spoken_lang


--- or insert 

insert into spoken_lang
select 
      f.value:language::String as Firat_lang,
      f.value:level::String as level_lang
from WORK1_DB.PUBLIC.JSON_RAW, table(flatten(RAW_FILE:spoken_languages)) as f;




-- Parquet data


    // Create file format and stage object
    
CREATE OR REPLACE FILE FORMAT MANAGE_DB.FILE_FORMAT.PARQUET_FORMAT
    TYPE = 'parquet';

CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
    url = 's3://snowflakeparquetdemo'   
    FILE_FORMAT = MANAGE_DB.FILE_FORMAT.PARQUET_FORMAT;
    
    
    // Preview the data
    
    
SELECT * FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE;
    


// File format in Queries

CREATE OR REPLACE STAGE MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
    url = 's3://snowflakeparquetdemo'  ;
    
SELECT * 
FROM @MANAGE_DB.EXTERNAL_STAGES.PARQUETSTAGE
(file_format => 'MANAGE_DB.FILE_FORMAT.PARQUET_FORMAT');