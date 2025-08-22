package pl.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PhishingDetectionJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        System.out.println("Using Kafka bootstrap servers: " + kafkaBootstrap);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("sms-for-phishing")
                .setGroupId("phishing-flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sms-scam")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: sms-for-phishing")
            .uid("kafka-source")
            // Here you could add phishing detection logic, for now just forward
            .map(value -> {
                System.out.println("Received for phishing detection: [" + value + "]");
                // TODO: Add phishing detection logic here
                return value;
            })
            .name("PhishingDetection")
            .uid("phishing-detection")
            .sinkTo(sink)
            .name("Kafka Sink: sms-scam")
            .uid("kafka-sink");

        env.execute("SMS Phishing Detection Job");
    }
}