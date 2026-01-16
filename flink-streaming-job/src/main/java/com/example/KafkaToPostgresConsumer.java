package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaToPostgresConsumer {

    public static void main(String[] args) {

        /* =====================
           Kafka configuration
           ===================== */
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "postgres-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(kafkaProps);

        consumer.subscribe(Collections.singletonList("output_topic"));

        /* =========================
           PostgreSQL configuration
           ========================= */
        String jdbcUrl = "jdbc:postgresql://localhost:5432/streaming_db";
        String dbUser = "user";
        String dbPassword = "password";

        String insertSql = "INSERT INTO processed_data (value) VALUES (?)";

        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
            PreparedStatement preparedStatement = connection.prepareStatement(insertSql)
        ) {
            System.out.println("Kafka → PostgreSQL consumer started...");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    String data = record.value();

                    preparedStatement.setString(1, data);
                    preparedStatement.executeUpdate();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
