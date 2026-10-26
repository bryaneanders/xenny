package xenny.ingest;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Configured from environment variables; see pipelines/trips/ingest.yaml. */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", null);
        Duration pollInterval = Duration.ofSeconds(Long.parseLong(env("POLL_SECONDS", "10")));

        Map<String, Object> serde = Map.of(
            "apicurio.registry.url", env("REGISTRY_URL", null),
            // Schemas are registered from git, never by the app.
            "apicurio.registry.auto-register", false,
            // Match our schema to the registered one regardless of JSON formatting.
            "apicurio.registry.canonicalize", true,
            "apicurio.registry.use-specific-avro-reader", true);

        Map<String, Object> producerConfig = new HashMap<>(serde);
        producerConfig.putAll(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd",
            ProducerConfig.LINGER_MS_CONFIG, 20,
            ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class));

        Map<String, Object> consumerConfig = new HashMap<>(serde);
        consumerConfig.putAll(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class));

        // Credentials come from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
        S3Client s3 = S3Client.builder()
            .endpointOverride(URI.create(env("S3_ENDPOINT", null)))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build();

        try (var producer = new KafkaProducer<String, Object>(producerConfig)) {
            Ledger ledger = Ledger.load(consumerConfig, producer, env("LEDGER_TOPIC", null));
            log.info("Loaded ledger with {} files", ledger.size());

            var ingestor = new Ingestor(s3, env("S3_BUCKET", null), env("S3_PREFIX", null), producer,
                env("TOPIC", null), ledger, Path.of(env("WORK_DIR", "/tmp")));
            while (true) {
                try {
                    ingestor.pollOnce();
                } catch (Exception e) {
                    // Files stay in place and are retried on the next poll.
                    log.error("Poll failed", e);
                }
                Thread.sleep(pollInterval);
            }
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            if (defaultValue == null) {
                throw new IllegalStateException("Missing environment variable " + name);
            }
            return defaultValue;
        }
        return value;
    }
}
