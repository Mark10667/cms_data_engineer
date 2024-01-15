
CMS_DATABASE_1

DESC INTEGRATION aws_s3_integration;

DROP DATABASE IF EXISTS cms_database_1;

CREATE DATABASE cms_database_1;

-- CREATE WAREHOUSE cms_warehouse;

CREATE SCHEMA cms_schema;

// Create Table
-- TRUNCATE TABLE cms_database_1.cms_schema.cms_pt_exp;

// create the patient experience table
CREATE OR REPLACE TABLE cms_database_1.cms_schema.cms_pt_exp (
Facility_Name STRING,
org_PAC_ID STRING,
measure_cd STRING,
measure_title STRING,
prf_rate FLOAT,
patient_count INT
);

// create the DAC table
CREATE OR REPLACE TABLE cms_database_1.cms_schema.cms_dac (
NPI STRING,
Ind_enrl_ID STRING,
Last_name STRING,
First_name STRING,
gndr STRING,
Cred STRING,
Med_sch STRING,
Grd_yr FLOAT,
pri_spec STRING,
sec_spec_all STRING,
Telehlth INT, 
org_pac_id STRING,
num_org_mem INT,
adr_ln_1 STRING,
City STRING,
State STRING,
ind_assgn STRING,
grp_assgn STRING,
adrs_id STRING,
Years_exp FLOAT,
zipcode STRING
);


// create the mips table
CREATE OR REPLACE TABLE cms_database_1.cms_schema.cms_mips (
NPI STRING, 
org_pac_id STRING, 
source STRING, 
Quality_category_score FLOAT,
PI_category_score FLOAT, 
IA_category_score FLOAT,
final_MIPS_score_without_CPB FLOAT,
final_MIPS_score FLOAT
);

// Create file format object
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format cms_database_1.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = '|'
    RECORD_DELIMITER = '\n'
    skip_header = 1
    empty_field_as_null = True 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
    -- error_on_column_count_mismatch = FALSE;
    
// Create staging schema
CREATE SCHEMA external_stage_schema;

// Create staging
-- DROP STAGE cms_database_1.external_stage_schema.cms_pt_exp_stage;

CREATE OR REPLACE STAGE cms_database_1.external_stage_schema.cms_pt_exp_stage 
    url="s3://cms-airflow-bucket/grp_cahps.csv"
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = cms_database_1.file_format_schema.format_csv;

list @cms_database_1.external_stage_schema.cms_pt_exp_stage;

CREATE OR REPLACE STAGE cms_database_1.external_stage_schema.cms_dac_stage 
    url="s3://cms-airflow-bucket/dac_clean.csv"
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = cms_database_1.file_format_schema.format_csv;

CREATE OR REPLACE STAGE cms_database_1.external_stage_schema.cms_mips_stage 
    url="s3://cms-airflow-bucket/mips_clean.csv"
    STORAGE_INTEGRATION = aws_s3_integration
    FILE_FORMAT = cms_database_1.file_format_schema.format_csv;

// Create schema for snowpipe
-- DROP SCHEMA cms_database_1.snowpipe_schema;
CREATE OR REPLACE SCHEMA cms_database_1.snowpipe_schema;

// Create Pipe
CREATE OR REPLACE PIPE cms_database_1.snowpipe_schema.cms_snowpipe
auto_ingest = TRUE
AS 
COPY INTO cms_database_1.cms_schema.cms_pt_exp
FROM @cms_database_1.external_stage_schema.cms_pt_exp_stage;


CREATE OR REPLACE PIPE cms_database_1.snowpipe_schema.cms_dac_snowpipe
auto_ingest = TRUE
AS 
COPY INTO cms_database_1.cms_schema.cms_dac
FROM @cms_database_1.external_stage_schema.cms_dac_stage;


CREATE OR REPLACE PIPE cms_database_1.snowpipe_schema.cms_mips_snowpipe
auto_ingest = TRUE
AS 
COPY INTO cms_database_1.cms_schema.cms_mips
FROM @cms_database_1.external_stage_schema.cms_mips_stage;


DESC PIPE cms_database_1.snowpipe_schema.cms_snowpipe;

ALTER PIPE cms_database_1.snowpipe_schema.cms_mips_snowpipe REFRESH;

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'cms_database_1.cms_schema.cms_mips', START_TIME => dateadd(hours, -1, current_timestamp())));

SELECT *
FROM cms_database_1.cms_schema.cms_pt_exp LIMIT 10;

SELECT COUNT(*) FROM cms_database_1.cms_schema.cms_pt_exp


SELECT *
FROM cms_database_1.cms_schema.cms_dac LIMIT 10;

SELECT COUNT(*) FROM cms_database_1.cms_schema.cms_dac

SELECT *
FROM cms_database_1.cms_schema.cms_mips LIMIT 10;

SELECT COUNT(*) FROM cms_database_1.cms_schema.cms_mips
