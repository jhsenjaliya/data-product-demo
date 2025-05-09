-- In Spark SQL (connected to Iceberg catalog)
CREATE TABLE prod.user_domain_snapshot (
    user_id STRING,
    profile STRING,
    preferences STRING,
    addresses STRING,
    status STRING,
    event_time TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_time));