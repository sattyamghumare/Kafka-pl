package com.example.kafkaspark.consumer;

import com.example.kafkaspark.config.KafkaConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import static org.apache.spark.sql.functions.*;

public class SparkConsumer {
    
    public static void main(String[] args) throws Exception {
        System.out.println("🎯 Starting Spark Streaming Consumer...");
        System.out.println("Listening to topic: " + KafkaConfig.TOPIC_NAME);
        System.out.println("================================================\n");
        
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
            .appName("KafkaSparkStreaming")
            .master("local[*]") // Run locally with all cores
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
        
        // Read stream from Kafka
        Dataset<Row> kafkaStream = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", KafkaConfig.BOOTSTRAP_SERVERS)
            .option("subscribe", KafkaConfig.TOPIC_NAME)
            .option("startingOffsets", "latest")
            .load();
        
        // Parse JSON messages
        Dataset<Row> messages = kafkaStream
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .select(
                col("key"),
                from_json(col("value"), 
                    "id STRING, content STRING, userId STRING, timestamp LONG, category STRING"
                ).as("data")
            )
            .select(
                col("key"),
                col("data.id"),
                col("data.content"),
                col("data.userId"),
                col("data.timestamp"),
                col("data.category"),
                from_unixtime(col("data.timestamp") / 1000).as("event_time")
            );
        
        // Aggregation 1: Count messages by category
        Dataset<Row> categoryCount = messages
            .groupBy("category")
            .count()
            .withColumnRenamed("count", "message_count")
            .orderBy(col("message_count").desc());
        
        // Aggregation 2: Count messages per user
        Dataset<Row> userActivity = messages
            .groupBy("userId")
            .count()
            .withColumnRenamed("count", "message_count")
            .orderBy(col("message_count").desc());
        
        // Write to console (for debugging)
        StreamingQuery query1 = categoryCount
            .writeStream()
            .outputMode("complete")
            .format("console")
            .queryName("Category Statistics")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
            .start();
        
        StreamingQuery query2 = userActivity
            .writeStream()
            .outputMode("complete")
            .format("console")
            .queryName("User Activity")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
            .start();
        
        // Write raw messages to console (optional)
        StreamingQuery rawStream = messages
            .writeStream()
            .outputMode("append")
            .format("console")
            .queryName("Raw Messages")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("2 seconds"))
            .start();
        
        System.out.println("✅ All streaming queries started!");
        System.out.println("📊 Watching for messages... (Press Ctrl+C to stop)\n");
        
        // Wait for queries to terminate
        query1.awaitTermination();
        query2.awaitTermination();
        rawStream.awaitTermination();
        
        spark.stop();
    }
}
