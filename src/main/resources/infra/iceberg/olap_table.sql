-- In Spark SQL (connected to Iceberg catalog)
./bin/spark-sql \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2 \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse="file:///data/spark_data/warehouse")


CREATE TABLE local.user_domain.user_profile_snapshot
(
    user_id STRING,
    profile STRING,
    preferences STRING,
    privacy STRING,
    compliance STRING,
    event_time   TIMESTAMP,
    ingestion_ts TIMESTAMP
) PARTITIONED BY (days(ingestion_ts));

CREATE TABLE local.user_domain.user_profile_latest
(
    user_id STRING,
    profile STRING,
    preferences STRING,
    privacy STRING,
    compliance STRING,
    event_time   TIMESTAMP,
    ingestion_ts TIMESTAMP
) PARTITIONED BY (days(ingestion_ts));

CREATE TABLE local.user_domain.business_event
(
    user_id STRING,
    profile STRING,
    preferences STRING,
    privacy STRING,
    compliance STRING,
    event_time   TIMESTAMP,
    ingestion_ts TIMESTAMP
) PARTITIONED BY (days(ingestion_ts));

spark.sql("CREATE TABLE local.user_domain.business_event
(
    user_id STRING,
    profile STRING,
    preferences STRING,
    privacy STRING,
    compliance STRING,
    event_time   TIMESTAMP,
    ingestion_ts TIMESTAMP
) PARTITIONED BY (days(ingestion_ts))")

INSERT INTO user_domain.business_event_snapshot
SELECT
    user_id,
    to_json(profile) as profile,
    to_json(preferences) as preferences,
    to_json(privacy) as privacy,
    to_json(compliance) as compliance,
    current_timestamp() as event_time,
    current_timestamp() as ingestion_ts
FROM kafka_stream_data;

