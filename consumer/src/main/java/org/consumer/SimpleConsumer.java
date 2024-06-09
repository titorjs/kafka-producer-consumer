package org.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class SimpleConsumer {
    public static void main(String[] args) {
        // Kafka consumer properties
        Scanner sc = new Scanner(System.in);
        String group = sc.nextLine();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka server address
        props.put("group.id", group);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create a Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the Kafka topic "test"
        consumer.subscribe(Collections.singletonList("javatechie-demo"));

        // Poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Received message: key = %s, value = %s, offset = %d%n",
                        record.key(), record.value(), record.offset());
            }
        }
    }
}
