-- In Spark SQL (connected to Iceberg catalog)
./bin/spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
--conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.local.type=hadoop --conf spark.sql.catalog.local.warehouse="file:///data/spark_data/warehouse"

truncate table local.user_domain.user_profile_snapshot;
truncate table local.user_domain.user_profile_latest;
truncate table local.marketing.marketing_profile_latest;


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


CREATE TABLE local.marketing.marketing_profile_latest
(
    user_id LONG,
    email STRING,
    zipcode STRING,
    notification_time STRING,
    marketing_consent STRING,
    eligibleAt       TIMESTAMP,
    event_time       TIMESTAMP,
    ingestion_ts     TIMESTAMP
) PARTITIONED BY (days(ingestion_ts));

