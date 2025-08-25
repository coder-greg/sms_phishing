package pl.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.webrisk.v1.WebRiskServiceClient;
import com.google.webrisk.v1.SearchUrisRequest;
import com.google.webrisk.v1.SearchUrisResponse;
import com.google.webrisk.v1.ThreatType;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.linkedin.urls.Url;
import com.linkedin.urls.detection.UrlDetector;
import com.linkedin.urls.detection.UrlDetectorOptions;

import java.util.ArrayList;
import java.util.List;

public class PhishingDetectionJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static class WebRiskEvaluator extends RichMapFunction<String, Tuple2<Boolean, String>> {
        private transient WebRiskServiceClient client;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.client = WebRiskServiceClient.create();
        }

        @Override
        public void close() throws Exception {
            if (this.client != null) {
                this.client.close();
            }
        }

        @Override
        public Tuple2<Boolean, String> map(String value) {
            boolean scam = false;
            try {
                JsonNode root = MAPPER.readTree(value);
                String message = root.path("message").asText("");

                if (message != null && !message.isEmpty()) {
                    UrlDetector detector = new UrlDetector(message, UrlDetectorOptions.Default);
                    List<Url> detected = detector.detect();

                    List<String> urls = new ArrayList<>();
                    for (Url u : detected) {
                        String url = u.getFullUrl();
                        if (url == null || url.isEmpty()) {
                            continue;
                        }
                        urls.add(url);

                        SearchUrisRequest request = SearchUrisRequest.newBuilder()
                                .setUri(url)
                                .addThreatTypes(ThreatType.MALWARE)
                                .addThreatTypes(ThreatType.SOCIAL_ENGINEERING)
                                .addThreatTypes(ThreatType.UNWANTED_SOFTWARE)
                                .build();

                        SearchUrisResponse response = client.searchUris(request);
                        if (response.hasThreat()) {
                            System.out.println("Scam detected " + url);
                            scam = true;
                            break;
                        }
                    }
                    if (!urls.isEmpty()) {
                        System.out.println("Detected URLs: " + urls);
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed during WebRisk evaluation: " + e.getMessage());
            }
            return Tuple2.of(scam, value);
        }
    }

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

        KafkaSink<String> sinkScam = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sms-scam")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<String> sinkSafe = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sms-out")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> input = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: sms-for-phishing")
                .uid("kafka-source");

        DataStream<Tuple2<Boolean, String>> evaluated = input
                .map(new WebRiskEvaluator())
                .name("PhishingDetection")
                .uid("phishing-detection");

        evaluated
                .filter(t -> t.f0)
                .map(t -> t.f1)
                .sinkTo(sinkScam)
                .name("Kafka Sink: sms-scam")
                .uid("kafka-sink-scam");

        evaluated
                .filter(t -> !t.f0)
                .map(t -> t.f1)
                .sinkTo(sinkSafe)
                .name("Kafka Sink: sms-out")
                .uid("kafka-sink-safe");

        env.execute("SMS Phishing Detection Job");
    }
}