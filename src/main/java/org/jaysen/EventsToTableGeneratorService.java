package org.jaysen;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class EventsToTableGeneratorService {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("EventsToTableGeneratorService")
//                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
//                .config("spark.sql.catalog.spark_catalog.type", "hadoop")
//                .config("spark.sql.catalog.spark_catalog.warehouse", "warehouse")
                .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.local.type", "hadoop")
                .config("spark.sql.catalog.local.warehouse", "file:///data/spark_data/warehouse")
                .config("datanucleus.schema.autoCreateAll", true)
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> kafkaRaw = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "user_domain.business_event")
                .option("startingOffsets", "latest")
                .load();

        Dataset<Row> parsed = kafkaRaw.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .withColumn("event_data", from_json(col("value"), new StructType()
                        .add("user_id", "string")
                        .add("profile", "string")
                        .add("preferences", "string")
                        .add("privacy", "string")
                        .add("compliance", "string")))
                .select(
                        col("event_data.user_id").alias("user_id"),
                        col("event_data.profile").alias("profile"),
                        col("event_data.preferences").alias("preferences"),
                        col("event_data.privacy").alias("privacy"),
                        col("event_data.compliance").alias("compliance"),
                        current_timestamp().alias("event_time"),
                        current_timestamp().alias("ingestion_ts")
                );
        // Append-only snapshot table
//        spark.read().table("local.user_domain.user_profile_snapshot").show();

        StreamingQuery snapshotQuery = parsed.writeStream()
                .format("iceberg")
                .outputMode("append")
                .option("checkpointLocation", "checkpoints/user_profile_snapshot")
                .toTable("local.user_domain.user_profile_snapshot");
//                .option("table", "local.user_domain.user_profile_snapshot")
//                .start();

//        // Upsert into latest table using foreachBatch
        StreamingQuery upsertQuery = parsed.writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    batchDF.createOrReplaceTempView("incoming_events_raw");
                    Dataset<Row> deduped = batchDF.sparkSession().sql(
                            "SELECT user_id, profile, preferences, privacy, compliance, event_time, ingestion_ts FROM (" +
                                    " SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) AS rn " +
                                    " FROM incoming_events_raw) WHERE rn = 1"
                    );

                    deduped.createOrReplaceTempView("incoming_events");

                    deduped.sparkSession().sql(
                            "MERGE INTO local.user_domain.user_profile_latest t " +
                                    "USING incoming_events s ON t.user_id = s.user_id " +
                                    "WHEN MATCHED THEN UPDATE SET * " +
                                    "WHEN NOT MATCHED THEN INSERT *"
                    );
                })
                .option("checkpointLocation", "checkpoints/user_profile_latest")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start();

        snapshotQuery.awaitTermination();
        upsertQuery.awaitTermination();
    }
}

