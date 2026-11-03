package xenny.cleaner;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import xenny.ref.Zone;
import xenny.trips.CleanTrip;
import xenny.trips.Trip;

/**
 * raw.trips -> drop duplicates -> join zones -> validate. Valid trips go to clean.trips;
 * rejects go to dlq.trips unchanged, with a "rule" header naming the rule they failed.
 */
final class Cleaner {

    static final String RAW = "raw.trips";
    static final String ZONES = "ref.zones";
    static final String CLEAN = "clean.trips";
    static final String DLQ = "dlq.trips";

    // Restart duplicates arrive minutes after the original, so a day is plenty.
    static final Duration DEDUPE_WINDOW = Duration.ofDays(1);
    static final String SEEN_STORE = "seen-keys";

    // Rule limits; see section 11 of sql/explore.sql.
    static final Duration MAX_DURATION = Duration.ofDays(1);
    static final double MAX_MILES = 100;

    /** The trip serde also writes dlq.trips, so it must use raw.trips' schema there. */
    record TripSerdes(Serde<Trip> trip, Serde<Zone> zone, Serde<CleanTrip> clean) {}

    /** A trip with its zones looked up; a null zone means the ID isn't in ref.zones. */
    record Checked(Trip trip, Zone pickup, Zone dropoff, String failedRule) {}

    static Topology build(TripSerdes serdes, MeterRegistry metrics) {
        var builder = new StreamsBuilder();
        builder.addStateStore(Stores.windowStoreBuilder(
            Stores.persistentWindowStore(SEEN_STORE, DEDUPE_WINDOW, DEDUPE_WINDOW, false),
            Serdes.String(), Serdes.Long()));
        GlobalKTable<String, Zone> zones = builder.globalTable(ZONES, Consumed.with(Serdes.String(), serdes.zone()),
            Materialized.as("zones"));

        Counter cleaned = metrics.counter("trips.cleaned");
        builder.stream(RAW, Consumed.with(Serdes.String(), serdes.trip()))
            .processValues(Dedupe::new, SEEN_STORE)
            .leftJoin(zones, (key, trip) -> zoneKey(trip.getPickupLocationId()),
                (trip, pickup) -> new Checked(trip, pickup, null, null))
            .leftJoin(zones, (key, c) -> zoneKey(c.trip().getDropoffLocationId()),
                (c, dropoff) -> new Checked(c.trip(), c.pickup(), dropoff, failedRule(c.trip(), c.pickup(), dropoff)))
            .split()
            .branch((key, c) -> c.failedRule() != null, Branched.withConsumer(rejected -> rejected
                .processValues(() -> new Reject(metrics))
                .to(DLQ, Produced.with(Serdes.String(), serdes.trip()))))
            .defaultBranch(Branched.withConsumer(valid -> valid
                .mapValues(Cleaner::toClean)
                .peek((key, trip) -> cleaned.increment())
                .to(CLEAN, Produced.with(Serdes.String(), serdes.clean()))));
        return builder.build();
    }

    /** The first rule the trip fails, or null if it passes them all. */
    static String failedRule(Trip t, Zone pickup, Zone dropoff) {
        // Refunds and voids: a negative copy of an earlier trip.
        if (negative(t.getFareAmount()) || negative(t.getTotalAmount())) {
            return "reversal";
        }
        Instant start = t.getPickupTime();
        Instant end = t.getDropoffTime();
        if (start == null || end == null || end.isBefore(start)
                || Duration.between(start, end).compareTo(MAX_DURATION) > 0) {
            return "bad_duration";
        }
        // Zero is allowed: flat-rate and flex-fare trips often don't record distance.
        Double miles = t.getTripDistance();
        if (miles != null && (miles < 0 || miles > MAX_MILES)) {
            return "bad_distance";
        }
        if (pickup == null || dropoff == null) {
            return "unknown_zone";
        }
        return null;
    }

    private static boolean negative(Double amount) {
        return amount != null && amount < 0;
    }

    private static String zoneKey(Integer locationId) {
        return locationId == null ? null : locationId.toString();
    }

    /** Copies the fields both schemas share, by name, then adds the zone names. */
    static CleanTrip toClean(Checked c) {
        var clean = new CleanTrip();
        for (Schema.Field f : Trip.getClassSchema().getFields()) {
            clean.put(f.name(), c.trip().get(f.pos()));
        }
        clean.setPickupBorough(c.pickup().getBorough());
        clean.setPickupZone(c.pickup().getZone());
        clean.setDropoffBorough(c.dropoff().getBorough());
        clean.setDropoffZone(c.dropoff().getZone());
        return clean;
    }

    /** Drops records whose key was already seen within the window, i.e. Phase 2's restart duplicates. */
    private static final class Dedupe implements FixedKeyProcessor<String, Trip, Trip> {
        private FixedKeyProcessorContext<String, Trip> context;
        private WindowStore<String, Long> seen;

        @Override
        public void init(FixedKeyProcessorContext<String, Trip> context) {
            this.context = context;
            this.seen = context.getStateStore(SEEN_STORE);
        }

        @Override
        public void process(FixedKeyRecord<String, Trip> record) {
            long time = record.timestamp();
            long window = DEDUPE_WINDOW.toMillis();
            try (var earlier = seen.fetch(record.key(), time - window, time + window)) {
                if (earlier.hasNext()) {
                    return;
                }
            }
            seen.put(record.key(), time, time);
            context.forward(record);
        }
    }

    /** Unwraps the original trip and adds the "rule" header. */
    private static final class Reject implements FixedKeyProcessor<String, Checked, Trip> {
        private final MeterRegistry metrics;
        private FixedKeyProcessorContext<String, Trip> context;

        Reject(MeterRegistry metrics) {
            this.metrics = metrics;
        }

        @Override
        public void init(FixedKeyProcessorContext<String, Trip> context) {
            this.context = context;
        }

        @Override
        public void process(FixedKeyRecord<String, Checked> record) {
            String rule = record.value().failedRule();
            metrics.counter("trips.rejected", "rule", rule).increment();
            record.headers().add("rule", rule.getBytes(UTF_8));
            context.forward(record.withValue(record.value().trip()));
        }
    }
}
