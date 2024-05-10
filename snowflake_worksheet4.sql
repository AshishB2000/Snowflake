-- Creating Warehouses for different departmemts


--for the data engineers
Create or replace warehouse DATAE_WH
with 
warehouse_size='small'
warehouse_type='standard'
auto_suspend=300
auto_resume=true
min_cluster_count=1
max_cluster_count=1
scaling_policy='standard';


--for data analysits
Create or replace warehouse D_ANA_WH
with 
warehouse_size='xsmall'
warehouse_type='standard'
auto_suspend=300
auto_resume=true
min_cluster_count=1
max_cluster_count=1
scaling_policy='standard';


-- creating roles for the WH

create or replace role Data_Engineer;
grant usage on warehouse DATAE_WH to role Data_Engineer;

create or replace role Data_Analyst ;
grant usage on warehouse D_ANA_WH to role Data_Analyst;



-- now creating the users(we can many users )

create user luke_DE password ='DE1' login_name='luke_de' default_role='Data_Engineer' default_warehouse = 'DATAE_WH' must_change_password=false;


grant role Data_Engineer to user luke_DE;