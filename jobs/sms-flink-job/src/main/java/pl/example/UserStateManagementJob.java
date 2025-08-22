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

    private static void handleOptInOptOut(Jedis jedis, String sender,  String message, String subscribedSetKey) {
        if (message == null ) {
            return ;
        }
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

    static class RoutedMessage {
        public String route;
        public String value;
        public RoutedMessage(String route, String value) {
            this.route = route;
            this.value = value;
        }
    }


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        System.out.println("Using Kafka bootstrap servers: " + kafkaBootstrap);
        String serviceNumber = System.getenv("SERVICE_NUMBER");
        System.out.println("Service number for opt-in/opt-out: " + serviceNumber);
        final String finalServiceNumber = serviceNumber;

        String redisHost = System.getenv("REDIS_HOST");
        String redisPortStr = System.getenv("REDIS_PORT");
        int redisPort = Integer.parseInt(redisPortStr);
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

        KafkaSink<String> phishingSink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("sms-for-phishing")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        var routedStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source: sms-in")
            .uid("kafka-source")
            .flatMap((String value, org.apache.flink.util.Collector<RoutedMessage> out) -> {
                System.out.println("Pulled from sms-in: [" + value + "]");
                System.out.println("Raw message length: " + (value != null ? value.length() : "null"));
        
                ObjectMapper mapper = new ObjectMapper();
                try (Jedis jedis = new Jedis(finalRedisHost, finalRedisPort)) {
                    System.out.println("About to parse JSON...");
                    JsonNode root = mapper.readTree(value);
                    System.out.println("Parsed JSON successfully: " + root.toString());
                    String sender = root.path("sender").asText(null);
                    String recipient = root.path("recipient").asText(null);
                    String message = root.path("message").asText(null);

                    System.out.println("Extracted SMS fields:");
                    System.out.println("  sender: " + sender);
                    System.out.println("  recipient: " + recipient);
                    System.out.println("  message: " + message);

                    String subscribedSetKey = "subscribed_numbers";
                    boolean isSubscribed = jedis.sismember(subscribedSetKey, recipient);
                    System.out.println("User is subscribed: " + isSubscribed);

                    if (recipient != null && recipient.equals(finalServiceNumber)) {
                        handleOptInOptOut(jedis, sender, message, subscribedSetKey);
                    }
                    if (isSubscribed) {
                        out.collect(new RoutedMessage("phishing", value));
                    } else {
                        out.collect(new RoutedMessage("out", value));
                    }
                } catch (Exception e) {
                    System.err.println("Failed to process SMS or Redis: " + e.getMessage());
                    e.printStackTrace(System.err);
                    System.err.println("Raw value that caused error: [" + value + "]");
                }
            })
            .returns(UserStateManagementJob.RoutedMessage.class)
            .name("Router")
            .uid("router");

        routedStream
            .filter(rm -> "out".equals(rm.route))
            .map(rm -> rm.value)
            .sinkTo(sink)
            .name("Kafka Sink: sms-out")
            .uid("kafka-sink");

        routedStream
            .filter(rm -> "phishing".equals(rm.route))
            .map(rm -> rm.value)
            .sinkTo(phishingSink)
            .name("Kafka Sink: sms-for-phishing")
            .uid("phishing-kafka-sink");

        env.execute("User State Management Job");
    }
}