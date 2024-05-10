--creating database & table
CREATE OR REPLACE DATABASE COPY_DB;


CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

--creating stage object
CREATE OR REPLACE SCHEMA EXT_STAGE


CREATE OR REPLACE STAGE COPY_DB.EXT_STAGE.aws_stage_copy
    url='s3://snowflakebucket-copyoption/size/';
  

 CREATE OR REPLACE FILE FORMAT COPY_DB.PUBLIC.FIRST_FILEFORMAT
 TYPE= csv field_delimiter=',' skip_header=1
    
--Load data using copy command using validation(it doesn't load any data it just return the errors)

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (FORMAT_NAME = COPY_DB.PUBLIC.FIRST_FILEFORMAT )
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;
    
SELECT * FROM ORDERS;


-- validating first 5 rows
    
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @COPY_DB.EXT_STAGE.AWS_STAGE_COPY
    file_format= (FORMAT_NAME = COPY_DB.PUBLIC.FIRST_FILEFORMAT )
    pattern='.*Order.*'
   VALIDATION_MODE = RETURN_5_ROWS ;




create or replace stage COPY_DB.EXT_STAGE.aws_stage_copy
    url ='s3://snowflakebucket-copyoption/returnfailed/';
    

--show all errors 
copy into copy_db.public.orders
    from @COPY_DB.EXT_STAGE.aws_stage_copy
    file_format= (FORMAT_NAME = COPY_DB.PUBLIC.FIRST_FILEFORMAT )
    pattern='.*Order.*'
    validation_mode=return_errors;




-- saving the rejected results in a table   
--RESULT_SCAN= RESULTS OF LAST 24 HOURS
-- LAST_QUERY_ID = ID FOR LAST QUERY

CREATE OR REPLACE TABLE REJECTED AS
SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
--OR

CREATE OR REPLACE TABLE REJECTED AS
SELECT * FROM TABLE(RESULT_SCAN('01b42de5-0305-1fc6-0005-b5d30001d1d2'));


CREATE OR REPLACE TABLE REJECTED AS
SELECT REJECTED_RECORD  FROM TABLE(RESULT_SCAN('01b42de5-0305-1fc6-0005-b5d30001d1d2'));

SELECT * FROM  REJECTED



-- IF WE DID ON_ERROR=CONTINUE WE CAN STILL GET THE ERRORS 
SELECT * FROM TABLE(VALIDATE(ORDERS,JOB_ID => '_LAST'))


SELECT * FROM  REJECTED

CREATE OR REPLACE TABLE REJECTED_VALES AS
SELECT 
SPLIT_PART(REJECTED_RECORD,',',1) AS ORDER_ID
FROM REJECTED;

SELECT * FROM REJECTED_VALES

-- using the size limit

CREATE OR REPLACE DATABASE COPY_DB;


CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

--creating stage object
CREATE OR REPLACE SCHEMA EXT_STAGE


CREATE OR REPLACE STAGE COPY_DB.EXT_STAGE.aws_stage_copy
    url='s3://snowflakebucket-copyoption/size/';
  

 CREATE OR REPLACE FILE FORMAT COPY_DB.PUBLIC.FIRST_FILEFORMAT
 TYPE= csv field_delimiter=',' skip_header=1
    
--Load data using copy command using validation(it doesn't load any data it just return the errors)
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (FORMAT_NAME = COPY_DB.PUBLIC.FIRST_FILEFORMAT )
    pattern='.*Order.*'
    size_limit= 20000;



-- return failed only (only can be used while using the on error=continue)
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));

    
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (FORMAT_NAME = COPY_DB.PUBLIC.FIRST_FILEFORMAT )
    pattern='.*Order.*'
    on_error= continue
    return_failed_only=true;


-- truncating the colmn(string truncate automatically to target column length when its set to true )
TRUNCATECOLUMNS = TRUE; 



--- LOAD HISTORY FROM INFORMATION SCHEMA 



SELECT * FROM WORK1_DB.INFORMATION_SCHEMA.LOAD_HISTORY


SELECT * FROM WORK1_DB.INFORMATION_SCHEMA.LOAD_HISTORY
    WHERE TABLE_NAME = 'ORDERS'

