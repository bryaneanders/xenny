package xenny.cleaner;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.sun.net.httpserver.HttpServer;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.avro.AvroSerde;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xenny.ref.Zone;

/** Configured from environment variables; see pipelines/trips/cleaner.yaml. */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP_SERVERS");

        Map<String, Object> serde = Map.of(
            "apicurio.registry.url", env("REGISTRY_URL"),
            // Schemas are registered from git, not the app.
            "apicurio.registry.auto-register", false,
            // Match our schema to the registered one regardless of JSON formatting.
            "apicurio.registry.canonicalize", true,
            "apicurio.registry.use-specific-avro-reader", true);

        // Before starting, so the zone table is complete when the first trip is joined.
        loadZones(bootstrap, serde, env("ZONES_URL"));

        // Raw trips written to dlq.trips keep raw.trips' schema rather than looking up dlq.trips'.
        Map<String, Object> tripSerde = new HashMap<>(serde);
        tripSerde.put("apicurio.registry.artifact.artifact-id", Cleaner.RAW + "-value");
        var serdes = new Cleaner.TripSerdes(avro(tripSerde), avro(serde), avro(serde));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trips-cleaner");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // Reads, state updates and writes commit together in one transaction.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR"));
        props.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "zstd");

        var metrics = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        var streams = new KafkaStreams(Cleaner.build(serdes, metrics), props);
        new KafkaStreamsMetrics(streams).bindTo(metrics);
        serveMetrics(metrics);

        var stopped = new CountDownLatch(1);
        streams.setStateListener((now, before) -> {
            log.info("State {} -> {}", before, now);
            if (now == KafkaStreams.State.ERROR || now == KafkaStreams.State.NOT_RUNNING) {
                stopped.countDown();
            }
        });
        // On SIGTERM: finish the current batch and commit. Kubernetes force-kills after 30s, so stay under that.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(20))));
        streams.start();

        // Runs until shut down or an unrecoverable error; exiting lets Kubernetes restart the pod.
        stopped.await();
        System.exit(1);
    }

    /** Publishes the zone lookup CSV to ref.zones, keyed by location ID. Rerunning rewrites the same keys. */
    private static void loadZones(String bootstrap, Map<String, Object> serde, String url) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient()
            .send(HttpRequest.newBuilder(URI.create(url)).build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("GET " + url + " returned " + response.statusCode());
        }
        // Rows look like: 1,"EWR","Newark Airport","EWR". No field contains a comma.
        List<String> rows = response.body().lines().skip(1).filter(line -> !line.isBlank()).toList();

        Map<String, Object> config = new HashMap<>(serde);
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
        try (var producer = new KafkaProducer<String, Zone>(config)) {
            for (String row : rows) {
                String[] f = row.strip().replace("\"", "").split(",");
                var zone = new Zone(Integer.parseInt(f[0]), f[1], f[2], f[3]);
                producer.send(new ProducerRecord<>(Cleaner.ZONES, f[0], zone)).get();
            }
        }
        log.info("Loaded {} zones into {}", rows.size(), Cleaner.ZONES);
    }

    /** Serves Prometheus metrics on :8080/metrics. */
    private static void serveMetrics(PrometheusMeterRegistry metrics) throws Exception {
        var server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/metrics", exchange -> {
            byte[] body = metrics.scrape().getBytes(UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
    }

    private static <T> AvroSerde<T> avro(Map<String, Object> config) {
        var serde = new AvroSerde<T>();
        serde.configure(config, false);
        return serde;
    }

    private static String env(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing environment variable " + name);
        }
        return value;
    }
}
