-- In Spark SQL (connected to Iceberg catalog)
CREATE TABLE user_domain.business_event
(
    user_id STRING,
    profile STRING,
    preferences STRING,
    privacy STRING,
    compliance STRING,
    event_time   TIMESTAMP,
    ingestion_ts TIMESTAMP
) PARTITIONED BY (hour(ingestion_ts))