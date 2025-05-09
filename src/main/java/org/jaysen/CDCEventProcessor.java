package org.jaysen;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;


public class CDCEventProcessor {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .appName("MarketingMetrics")
                .config("spark.cassandra.connection.host", "scylla-db-host")
                .getOrCreate();

        Dataset<Row> userCDC = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker:9092")
                .option("subscribe", "user-domain-cdc-events")
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), userSchema()).as("data"))
                .select("data.*");

        Dataset<Row> transactionCDC = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker:9092")
                .option("subscribe", "transaction-domain-cdc-events")
                .load()
                .selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), transactionSchema()).as("data"))
                .select("data.*");

        // Compute User Engagement Rate (logins per user)
        Dataset<Row> userLogins = userCDC.filter(col("changed_fields.auth_last_login").isNotNull())
                .groupBy(col("key.user_id"))
                .agg(count("*").alias("login_count"));

        // Compute Active Users per Timezone
        Dataset<Row> activeUsers = userCDC.filter(col("changed_fields.preferences_timezone").isNotNull())
                .groupBy(col("changed_fields.preferences_timezone"))
                .agg(countDistinct("key.user_id").alias("active_users"));

        // Compute Transaction Conversion Rate
        Dataset<Row> completedTransactions = transactionCDC
                .filter(col("changed_fields.payment_status").equalTo("completed"))
                .agg(count("*").alias("completed"));

        Dataset<Row> initiatedTransactions = transactionCDC
                .filter(col("changed_fields.payment_status").equalTo("initiated"))
                .agg(count("*").alias("initiated"));

        Dataset<Row> conversionRate = completedTransactions.crossJoin(initiatedTransactions)
                .withColumn("conversion_rate",
                        expr("completed / NULLIF(initiated, 0)"));

        // Persisting Results into ScyllaDB
        StreamingQuery query1 = userLogins.writeStream()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "marketing")
                .option("table", "user_engagement_metrics")
                .option("checkpointLocation", "/tmp/checkpoint/user_engagement")
                .outputMode("complete")
                .start();

        StreamingQuery query2 = activeUsers.writeStream()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "marketing")
                .option("table", "active_user_metrics")
                .option("checkpointLocation", "/tmp/checkpoint/active_users")
                .outputMode("complete")
                .start();

        StreamingQuery query3 = conversionRate.writeStream()
                .format("org.apache.spark.sql.cassandra")
                .option("keyspace", "marketing")
                .option("table", "conversion_metrics")
                .option("checkpointLocation", "/tmp/checkpoint/conversion_rate")
                .outputMode("complete")
                .start();

        query1.awaitTermination();
        query2.awaitTermination();
        query3.awaitTermination();
    }

    static StructType userSchema() {
        return new StructType()
                .add("table", "string")
                .add("key", new StructType().add("user_id", "string"))
                .add("operation", "string")
                .add("timestamp", "string")
                .add("changed_fields", new StructType()
                        .add("auth_last_login", "string")
                        .add("auth_login_count", "integer")
                        .add("preferences_timezone", "string"));
    }

    static StructType transactionSchema() {
        return new StructType()
                .add("table", "string")
                .add("key", new StructType().add("transaction_id", "string"))
                .add("operation", "string")
                .add("timestamp", "string")
                .add("changed_fields", new StructType()
                        .add("payment_status", "string"));
    }
}

