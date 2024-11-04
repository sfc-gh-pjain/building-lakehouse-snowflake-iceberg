use schema iceberg1.demo;
use role accountadmin;

CREATE WAREHOUSE IF NOT EXISTS LAB_S_WH 
    WITH WAREHOUSE_SIZE = 'SMALL' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE 
    COMMENT = 'WAREHOUSE CREATED FOR DEMO';

use warehouse lab_s_wh;


-- Create and integrate Snowflake with your obejct storage 

CREATE EXTERNAL VOLUME if not exists extvol_managed_icbg_demo
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'managed-iceberg-storage'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 'S3 Bucket'
            STORAGE_AWS_ROLE_ARN = 'AWS ARN for ROLE'
         )
      );

desc EXTERNAL VOLUME extvol_managed_icbg_demo; 


/*************************************
-- Bronze
*************************************/
-- Load the data files into the stage either using the Snowflake UI or using the PUT command or use the External stage in S3
-- https://docs.snowflake.com/en/sql-reference/sql/put


create stage if not exists json_stage;
ls @json_stage;

create or replace file format json_format
  type = json
  --compression = GZIP
  STRIP_OUTER_ARRAY= TRUE
  TRIM_SPACE = TRUE;


  CREATE or replace ICEBERG  table snow_icbg_sales_raw (
	customer_id NUMBER(20,0) ,
    customer_name STRING,
    purchases object(prodid number(20,2), purchase_amount number(20,2), purchase_date date, quantity number(5,0))
)

  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='extvol_managed_icbg_demo'
  BASE_LOCATION='RELATIVE/PATH/to/ICEBERG'
; 

desc table snow_icbg_sales_raw;

-- Copy data into the table using the schema detection support
COPY INTO snow_icbg_sales_raw
  FROM @json_stage/sales_data.json
  FILE_FORMAT = (FORMAT_NAME = 'json_format')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';

SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('iceberg1.demo.snow_icbg_sales_raw');

select * from snow_icbg_sales_raw limit 10;



/*************************************
--- Gold Layer/Consumption Layer
*************************************/

select 
    s.customer_id,
    s.customer_name,
    s.purchases:"prodid"::number(5) as product_id,
    s.purchases:"purchase_amount"::number(10) as saleprice,
    s.purchases:"quantity"::number(5) as quantity,
    s.purchases:"purchase_date"::date as salesdate
from 
    snow_icbg_sales_raw s;

SELECT
  SYSTEM$TYPEOF(purchases) from snow_icbg_sales_raw;


  CREATE or replace ICEBERG table snow_icbg_sale_tranformed  
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'extvol_managed_icbg_demo'
  BASE_LOCATION = 'RELATIVE/PATH/to/ICEBERG'
  
  as
(
select 
    s.customer_id,
    s.customer_name,
    s.purchases:"prodid"::number(5) as product_id,
    s.purchases:"purchase_amount"::number(10) as saleprice,
    s.purchases:"quantity"::number(5) as quantity,
    s.purchases:"purchase_date"::date as salesdate
from 
    snow_icbg_sales_raw s
)
;    

select * from snow_icbg_sale_tranformed limit 10;


------------- Schema Evolution --------------

ALTER ICEBERG TABLE snow_icbg_sale_tranformed ADD COLUMN UNITPRICE NUMBER(10,4);

desc iceberg table snow_icbg_sale_tranformed;

UPDATE snow_icbg_sale_tranformed
SET 
UNITPRICE = (SALEPRICE/QUANTITY);



SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('iceberg1.demo.snow_icbg_sale_tranformed');  

select * from snow_icbg_sale_tranformed;


-------- Time Travel --------------

SELECT * FROM snow_icbg_sale_tranformed AT(OFFSET => -60*2);


-- Cleanup
--DROP ICEBERG TABLE if exists snow_icbg_sales_raw;
--DROP ICEBERG TABLE if exists snow_icbg_sale_tranformed;

--rm @stg_icbg_demo/RELATIVE/PATH/to/ICEBERG ;
--rm @stg_icbg_demo/RELATIVE/PATH/to/ICEBERG;
