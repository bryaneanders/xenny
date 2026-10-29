package xenny.ingest;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import xenny.trips.Trip;

/**
 * Turns each file under bucket/prefix into one Kafka record per row, once per file.
 * Finished files move to processed/, unreadable ones to failed/.
 */
final class Ingestor {

    private static final Logger log = LoggerFactory.getLogger(Ingestor.class);

    private final S3Client s3;
    private final String bucket;
    private final String prefix;
    private final Producer<String, Object> producer;
    private final String topic;
    private final Ledger ledger;
    private final Path workFile;

    Ingestor(S3Client s3, String bucket, String prefix, Producer<String, Object> producer, String topic,
             Ledger ledger, Path workDir) {
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = prefix;
        this.producer = producer;
        this.topic = topic;
        this.ledger = ledger;
        this.workFile = workDir.resolve("current.parquet");
    }

    /** Ingests every file currently in the prefix (not its subfolders). */
    void pollOnce() throws Exception {
        var listing = s3.listObjectsV2Paginator(b -> b.bucket(bucket).prefix(prefix).delimiter("/"));
        for (S3Object obj : listing.contents()) {
            if (!obj.key().endsWith("/")) {
                ingest(obj.key());
            }
        }
    }

    private void ingest(String key) throws Exception {
        String sha256 = download(key);

        LedgerEntry entry = ledger.get(key);
        if (entry != null) {
            if (entry.getSha256().toString().equals(sha256)) {
                log.info("{} was already ingested; moving to processed/", key);
                move(key, "processed/");
            } else {
                // Row keys are file:row, so new content under an old name would collide downstream.
                log.warn("{} was already ingested with different content; moving to failed/", key);
                move(key, "failed/");
            }
            return;
        }

        log.info("Ingesting {}", key);
        long rows;
        try {
            rows = produce(key);
        } catch (TripReader.BadFileException e) {
            log.error("{} can't be read; moving to failed/", key, e);
            move(key, "failed/");
            return;
        }

        // Only after every row is acknowledged, so a crash before this point re-ingests the file.
        ledger.record(key, new LedgerEntry(sha256, rows, Instant.now()));
        move(key, "processed/");
        log.info("Ingested {}: {} rows", key, rows);
    }

    /** Sends every row and waits for all acknowledgements. Returns the row count. */
    private long produce(String key) throws Exception {
        var sendError = new AtomicReference<Exception>();
        long rows = 0;
        try (var reader = new TripReader(workFile, key, Instant.now())) {
            for (Trip trip = reader.next(); trip != null; trip = reader.next()) {
                producer.send(new ProducerRecord<>(topic, key + ":" + trip.getRowNumber(), trip), (meta, e) -> {
                    if (e != null) {
                        sendError.compareAndSet(null, e);
                    }
                });
                rows++;
            }
        }
        producer.flush();
        if (sendError.get() != null) {
            throw sendError.get();
        }
        return rows;
    }

    /** Downloads the object to the work file and returns its SHA-256. */
    private String download(String key) throws Exception {
        Files.deleteIfExists(workFile);
        s3.getObject(b -> b.bucket(bucket).key(key), ResponseTransformer.toFile(workFile));
        return sha256(workFile);
    }

    private void move(String key, String folder) {
        String target = folder + key;
        s3.copyObject(b -> b.sourceBucket(bucket).sourceKey(key).destinationBucket(bucket).destinationKey(target));
        s3.deleteObject(b -> b.bucket(bucket).key(key));
    }

    private static String sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = new DigestInputStream(Files.newInputStream(file), digest)) {
            in.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
