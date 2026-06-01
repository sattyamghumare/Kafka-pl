package com.example.kafkaspark;

import com.example.kafkaspark.producer.MessageProducer;
import com.example.kafkaspark.consumer.SparkConsumer;

public class KafkaSparkApplication {
    
    public static void main(String[] args) {
        System.out.println("🚀 Starting Kafka + Spark Streaming Application");
        System.out.println("===============================================");
        
        // Note: Normally you would run producer and consumer separately
        // For demo, showing both components
        
        System.out.println("\n📤 Kafka Producer is ready to send messages");
        System.out.println("📥 Spark Consumer is ready to process streams");
        System.out.println("\n💡 To test:");
        System.out.println("1. Run MessageProducer.main() to send messages");
        System.out.println("2. Run SparkConsumer.main() to consume and process");
        
        System.out.println("\n✅ Application setup complete!");
        
        // Uncomment to run producer:
        // MessageProducer.main(null);
        
        // Uncomment to run consumer:
        // SparkConsumer.main(null);
    }
}
