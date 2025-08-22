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

public class UserStateManagementJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        System.out.println("Using Kafka bootstrap servers: " + kafkaBootstrap);
        String serviceNumber = System.getenv("SERVICE_NUMBER");
        System.out.println("Service number for opt-in/opt-out: " + serviceNumber);
        final String finalServiceNumber = serviceNumber;

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
                System.out.println("Pulled from sms-in: " + value);

                ObjectMapper mapper = new ObjectMapper();
                try (Jedis jedis = new Jedis(finalRedisHost, finalRedisPort)) {
                    JsonNode root = mapper.readTree(value);
                    String sender = root.path("sender").asText(null);
                    String recipient = root.path("recipient").asText(null);
                    String message = root.path("message").asText(null);
                    if (recipient == null || !recipient.equals(finalServiceNumber)) {
                        System.out.println("Skipping message: recipient " + recipient + " does not match service number " + finalServiceNumber);
                        return value;
                    }

                    System.out.println("Extracted SMS fields:");
                    System.out.println("  sender: " + sender);
                    System.out.println("  recipient: " + recipient);
                    System.out.println("  message: " + message);

                    String subscribedSetKey = "subscribed_numbers";
                    if (message != null) {
                        if ("START".equalsIgnoreCase(message.trim())) {
                            Long added = jedis.sadd(subscribedSetKey, sender);
                            if (added != null && added > 0) {
                                System.out.println("Added " + sender + " to subscribed numbers.");
                            } else {
                                System.out.println(sender + " was already subscribed.");
                            }
                        } else if ("STOP".equalsIgnoreCase(message.trim())) {
                            Long removed = jedis.srem(subscribedSetKey, sender);
                            if (removed != null && removed > 0) {
                                System.out.println("Removed " + sender + " from subscribed numbers.");
                            } else {
                                System.out.println(sender + " was not subscribed.");
                            }
                        }
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