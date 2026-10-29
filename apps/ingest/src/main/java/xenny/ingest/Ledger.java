package xenny.ingest;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Files already ingested, kept in a compacted topic keyed by object key.
 * Loaded into memory at startup; it stays small (one entry per file).
 */
final class Ledger {

    private final Map<String, LedgerEntry> entries;
    private final Producer<String, Object> producer;
    private final String topic;

    private Ledger(Map<String, LedgerEntry> entries, Producer<String, Object> producer, String topic) {
        this.entries = entries;
        this.producer = producer;
        this.topic = topic;
    }

    /** Reads the topic from the beginning to its current end. */
    static Ledger load(Map<String, Object> consumerConfig, Producer<String, Object> producer, String topic) {
        Map<String, LedgerEntry> entries = new HashMap<>();
        try (var consumer = new KafkaConsumer<String, LedgerEntry>(consumerConfig)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                for (ConsumerRecord<String, LedgerEntry> r : consumer.poll(Duration.ofSeconds(1))) {
                    if (r.value() == null) {
                        entries.remove(r.key());
                    } else {
                        entries.put(r.key(), r.value());
                    }
                }
            }
        }
        return new Ledger(entries, producer, topic);
    }

    LedgerEntry get(String file) {
        return entries.get(file);
    }

    /** Writes the entry and waits for Kafka to acknowledge it. */
    void record(String file, LedgerEntry entry) throws Exception {
        producer.send(new ProducerRecord<>(topic, file, entry)).get();
        entries.put(file, entry);
    }

    int size() {
        return entries.size();
    }
}
