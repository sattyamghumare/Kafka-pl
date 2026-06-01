package com.example.kafkaspark.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaConfig {
    
    // Kafka connection settings
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC_NAME = "message-stream";
    
    // Get Producer configuration
    public static Properties getProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        return props;
    }
    
    // Spark streaming configuration
    public static String getKafkaReadConfig() {
        return String.format("kafka.bootstrap.servers=%s,subscribe=%s,startingOffsets=earliest",
                BOOTSTRAP_SERVERS, TOPIC_NAME);
    }
}
