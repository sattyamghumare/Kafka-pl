package com.example.kafkaspark.producer;

import com.example.kafkaspark.config.KafkaConfig;
import com.example.kafkaspark.model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Random;
import java.util.UUID;

public class MessageProducer {
    
    private static final String[] CONTENTS = {
        "Hello Kafka!", "Spark is awesome!", "Streaming data in real-time",
        "This is a test message", "Kafka + Spark = ❤️", "Processing logs",
        "User activity detected", "System alert", "Transaction completed",
        "New user registered"
    };
    
    private static final String[] CATEGORIES = {
        "info", "warning", "error", "debug", "trace"
    };
    
    private static final String[] USERS = {
        "user123", "user456", "user789", "user001", "user002"
    };
    
    public static void main(String[] args) {
        System.out.println("📡 Starting Kafka Producer...");
        System.out.println("Topic: " + KafkaConfig.TOPIC_NAME);
        System.out.println("Broker: " + KafkaConfig.BOOTSTRAP_SERVERS);
        System.out.println("===============================================\n");
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProducerProperties())) {
            Random random = new Random();
            int messageCount = 20; // Sending 20 messages
            
            for (int i = 1; i <= messageCount; i++) {
                // Create random message
                String content = CONTENTS[random.nextInt(CONTENTS.length)];
                String userId = USERS[random.nextInt(USERS.length)];
                String category = CATEGORIES[random.nextInt(CATEGORIES.length)];
                
                Message message = new Message(
                    UUID.randomUUID().toString(),
                    content + " - #" + i,
                    userId,
                    System.currentTimeMillis(),
                    category
                );
                
                String jsonMessage = message.toJson();
                
                // Send to Kafka
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    KafkaConfig.TOPIC_NAME, 
                    message.getUserId(), // key
                    jsonMessage          // value
                );
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("✅ Sent message %d | Topic: %s | Partition: %d | Offset: %d | Category: %s%n",
                            i, metadata.topic(), metadata.partition(), metadata.offset(), message.getCategory());
                    } else {
                        System.err.println("❌ Error sending message: " + exception.getMessage());
                    }
                });
                
                Thread.sleep(500); // Send one message every 500ms
            }
            
            producer.flush();
            System.out.println("\n✅ All " + messageCount + " messages sent successfully!");
            
        } catch (Exception e) {
            System.err.println("❌ Producer error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
