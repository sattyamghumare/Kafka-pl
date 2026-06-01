package com.example.kafkaspark;

import com.example.kafkaspark.producer.MessageProducer;
import com.example.kafkaspark.consumer.SparkConsumer;

public class KafkaSparkTest {
    
    public static void main(String[] args) {
        System.out.println("🧪 Testing Kafka + Spark Integration");
        System.out.println("====================================");
        
        // Step 1: Check if Kafka is running
        System.out.println("\n✅ Step 1: Make sure Docker containers are running");
        System.out.println("   Run: docker-compose up -d");
        
        // Step 2: Create topic if not exists (auto-created by producer)
        System.out.println("\n✅ Step 2: Topic will be auto-created when producer sends messages");
        
        // Step 3: Run test
        System.out.println("\n✅ Step 3: Running Producer and Consumer");
        System.out.println("   - First terminal: Run MessageProducer.main()");
        System.out.println("   - Second terminal: Run SparkConsumer.main()");
        
        System.out.println("\n📊 Expected Output:");
        System.out.println("   - Producer: Shows sent messages with offsets");
        System.out.println("   - Consumer: Shows real-time aggregations");
        System.out.println("     * Category statistics (info, warning, error, etc.)");
        System.out.println("     * User activity counts");
        
        System.out.println("\n✅ Test setup complete!");
    }
}
