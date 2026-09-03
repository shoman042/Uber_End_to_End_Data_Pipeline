CREATE DATABASE IF NOT EXISTS uber_pipeline;
USE uber_pipeline;

-- ========== SILVER ==========
CREATE EXTERNAL TABLE IF NOT EXISTS silver_unified_rides (
    trip_id           STRING,
    start_time        STRING,
    start_geo_hash    STRING,
    end_geo_hash      STRING,
    trip_duration_sec BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/silver/unified_rides'
TBLPROPERTIES ("skip.header.line.count"="1");

-- ========== GOLD ==========
CREATE EXTERNAL TABLE IF NOT EXISTS gold_dim_date (
    date_key      INT,
    full_date     DATE,
    day           INT,
    day_name      STRING,
    day_of_week   INT,
    is_weekend    INT,
    week_of_year  INT,
    month         INT,
    month_name    STRING,
    quarter       INT,
    year          INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/gold/dim_date'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS gold_dim_time (
    time_key       INT,
    hour           INT,
    minute         INT,
    period         STRING,
    is_rush_hour   INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/gold/dim_time'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS gold_dim_geography (
    geo_hash          STRING,
    lat               DOUBLE,
    lon               DOUBLE,
    parent_geo_res6   STRING,
    parent_geo_res5   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/gold/dim_geography'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS gold_dim_duration_bucket (
    duration_bucket_key INT,
    bucket_label        STRING,
    min_seconds         INT,
    max_seconds         INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/gold/dim_duration_bucket'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS gold_fact_trip (
    trip_id             STRING,
    date_key            INT,
    time_key            INT,
    start_geo_key       STRING,
    end_geo_key         STRING,
    duration_bucket_key INT,
    start_time          STRING,
    trip_duration_sec   BIGINT,
    trip_count          INT,
    is_same_cell        INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://localhost:9000/home/student/uber_pipeline/gold/fact_trip'
TBLPROPERTIES ("skip.header.line.count"="1");
