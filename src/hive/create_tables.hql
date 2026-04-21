-- =============================================================================
-- create_tables.hql
-- Creates the healthcare Hive database and all six external tables.
--
-- Run via beeline from inside the hive-server container:
--   beeline -u jdbc:hive2://localhost:10000 -n root -f /home/src/hive/create_tables.hql
-- =============================================================================

-- Step 1: Create database
CREATE DATABASE IF NOT EXISTS healthcare
    COMMENT 'Synthea patient data for the healthcare recommendation system';

-- Step 2: Switch to the database
USE healthcare;

-- ==============================
-- patients
-- ==============================
DROP TABLE IF EXISTS patients;

CREATE EXTERNAL TABLE patients (
    Id                   STRING,
    BIRTHDATE            STRING,
    DEATHDATE            STRING,
    SSN                  STRING,
    DRIVERS              STRING,
    PASSPORT             STRING,
    PREFIX               STRING,
    FIRST                STRING,
    LAST                 STRING,
    SUFFIX               STRING,
    MAIDEN               STRING,
    MARITAL              STRING,
    RACE                 STRING,
    ETHNICITY            STRING,
    GENDER               STRING,
    BIRTHPLACE           STRING,
    ADDRESS              STRING,
    CITY                 STRING,
    STATE                STRING,
    COUNTY               STRING,
    ZIP                  STRING,
    LAT                  DOUBLE,
    LON                  DOUBLE,
    HEALTHCARE_EXPENSES  DOUBLE,
    HEALTHCARE_COVERAGE  DOUBLE
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/patients'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- conditions
-- ==============================
DROP TABLE IF EXISTS conditions;

CREATE EXTERNAL TABLE conditions (
    START           STRING,
    STOP            STRING,
    PATIENT         STRING,
    ENCOUNTER       STRING,
    CODE            STRING,
    DESCRIPTION     STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/conditions'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- medications
-- ==============================
DROP TABLE IF EXISTS medications;

CREATE EXTERNAL TABLE medications (
    START               STRING,
    STOP                STRING,
    PATIENT             STRING,
    PAYER               STRING,
    ENCOUNTER           STRING,
    CODE                STRING,
    DESCRIPTION         STRING,
    BASE_COST           DOUBLE,
    PAYER_COVERAGE      DOUBLE,
    DISPENSES           INT,
    TOTALCOST           DOUBLE,
    REASONCODE          STRING,
    REASONDESCRIPTION   STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/medications'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- observations
-- ==============================
DROP TABLE IF EXISTS observations;

CREATE EXTERNAL TABLE observations (
    DATE            STRING,
    PATIENT         STRING,
    ENCOUNTER       STRING,
    CODE            STRING,
    DESCRIPTION     STRING,
    VALUE           STRING,
    UNITS           STRING,
    TYPE            STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/observations'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- encounters
-- ==============================
DROP TABLE IF EXISTS encounters;

CREATE EXTERNAL TABLE encounters (
    Id                      STRING,
    START                   STRING,
    STOP                    STRING,
    PATIENT                 STRING,
    ORGANIZATION            STRING,
    PROVIDER                STRING,
    PAYER                   STRING,
    ENCOUNTERCLASS          STRING,
    CODE                    STRING,
    DESCRIPTION             STRING,
    BASE_ENCOUNTER_COST     DOUBLE,
    TOTAL_CLAIM_COST        DOUBLE,
    PAYER_COVERAGE          DOUBLE,
    REASONCODE              STRING,
    REASONDESCRIPTION       STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/encounters'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- procedures
-- ==============================
DROP TABLE IF EXISTS procedures;

CREATE EXTERNAL TABLE procedures (
    START               STRING,
    STOP                STRING,
    PATIENT             STRING,
    ENCOUNTER           STRING,
    CODE                STRING,
    DESCRIPTION         STRING,
    BASE_COST           DOUBLE,
    REASONCODE          STRING,
    REASONDESCRIPTION   STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/data/raw/procedures'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ==============================
-- Verification
-- ==============================
SELECT 'patients'     AS table_name, COUNT(*) AS row_count FROM patients     UNION ALL
SELECT 'conditions'   AS table_name, COUNT(*) AS row_count FROM conditions   UNION ALL
SELECT 'medications'  AS table_name, COUNT(*) AS row_count FROM medications  UNION ALL
SELECT 'observations' AS table_name, COUNT(*) AS row_count FROM observations UNION ALL
SELECT 'encounters'   AS table_name, COUNT(*) AS row_count FROM encounters   UNION ALL
SELECT 'procedures'   AS table_name, COUNT(*) AS row_count FROM procedures;