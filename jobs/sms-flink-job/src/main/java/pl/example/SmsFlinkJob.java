package pl.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import redis.clients.jedis.Jedis;

public class SmsFlinkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (kafkaBootstrap == null || kafkaBootstrap.trim().isEmpty()) {
            kafkaBootstrap = "kafka:29092";
        }
        System.out.println("Using Kafka bootstrap servers: " + kafkaBootstrap);

        // Redis connection info from env
        String redisHost = System.getenv("REDIS_HOST");
        String redisPortStr = System.getenv("REDIS_PORT");
        int redisPort = 6379;
        if (redisHost == null || redisHost.trim().isEmpty()) {
            redisHost = "redis";
        }
        if (redisPortStr != null && !redisPortStr.trim().isEmpty()) {
            try {
                redisPort = Integer.parseInt(redisPortStr);
            } catch (NumberFormatException e) {
                System.err.println("Invalid REDIS_PORT, using default 6379");
            }
        }
        final String finalRedisHost = redisHost;
        final int finalRedisPort = redisPort;

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics("sms-in")
                .setGroupId("sms-flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

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

        env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: sms-in")
            .uid("kafka-source")
            .map(value -> {
                // Log when SMS is pulled from sms-in topic
                System.out.println("Pulled from sms-in: " + value);

                // Parse JSON and extract fields
                ObjectMapper mapper = new ObjectMapper();
                try (Jedis jedis = new Jedis(finalRedisHost, finalRedisPort)) {
                    JsonNode root = mapper.readTree(value);
                    String sender = root.path("sender").asText(null);
                    String recipient = root.path("recipient").asText(null);
                    String message = root.path("message").asText(null);
                    String action = root.path("action").asText(null); // "opt_in" or "opt_out"

                    // Example action: log extracted fields
                    System.out.println("Extracted SMS fields:");
                    System.out.println("  sender: " + sender);
                    System.out.println("  recipient: " + recipient);
                    System.out.println("  message: " + message);
                    System.out.println("  action: " + action);

                    // Business logic: opt-in/opt-out using Redis set
                    String setKey = "subscribed_users";
                    if (sender != null && action != null) {
                        if (action.equalsIgnoreCase("opt_in")) {
                            jedis.sadd(setKey, sender);
                            System.out.println("Opt-in: added " + sender + " to " + setKey);
                        } else if (action.equalsIgnoreCase("opt_out")) {
                            jedis.srem(setKey, sender);
                            System.out.println("Opt-out: removed " + sender + " from " + setKey);
                        }
                        // Optionally, you can check membership:
                        boolean isSubscribed = jedis.sismember(setKey, sender);
                        System.out.println("Is " + sender + " subscribed? " + isSubscribed);
                    }

                } catch (Exception e) {
                    System.err.println("Failed to process SMS or Redis: " + e.getMessage());
                }
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