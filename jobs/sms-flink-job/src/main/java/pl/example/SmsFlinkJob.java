package pl.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SmsFlinkJob {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka bootstrap servers: overridable via env var KAFKA_BOOTSTRAP_SERVERS
        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrap == null || kafkaBootstrap.trim().isEmpty()) {
            kafkaBootstrap = "kafka:29092";
        }
        System.out.println("Using Kafka bootstrap servers: " + kafkaBootstrap);

        // Kafka connectors configured via builder APIs (no Properties object required)
        
        // Kafka source: reads from "sms-in" topic (new Source API)
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("sms-in")
                .setGroupId("sms-flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Kafka sink: writes to "sms-out" topic (new Sink API)
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sms-out")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Stream pipeline: read from Kafka, log, write to Kafka
        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: sms-in")
            .uid("kafka-source")
            .map(value -> {
                System.out.println("Received: " + value);
                return value;
            })
            .name("Logger")
            .uid("logger")
            .sinkTo(sink)
            .name("Kafka Sink: sms-out")
            .uid("kafka-sink");

        env.execute("SMS Flink Kafka Pass-Through Job");
    }
}