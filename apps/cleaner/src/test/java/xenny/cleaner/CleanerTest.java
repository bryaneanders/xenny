package xenny.cleaner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Properties;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import xenny.ref.Zone;
import xenny.trips.CleanTrip;
import xenny.trips.Trip;

class CleanerTest {

    // Plain Avro serdes; the real ones need a schema registry.
    private static final Serde<Trip> TRIP = serde(Trip.getEncoder(), Trip.getDecoder());
    private static final Serde<Zone> ZONE = serde(Zone.getEncoder(), Zone.getDecoder());
    private static final Serde<CleanTrip> CLEAN = serde(CleanTrip.getEncoder(), CleanTrip.getDecoder());

    private static final Instant PICKUP = Instant.parse("2026-08-01T04:03:04Z");

    private TopologyTestDriver driver;
    private TestInputTopic<String, Trip> raw;
    private TestOutputTopic<String, CleanTrip> clean;
    private TestOutputTopic<String, Trip> dlq;

    @BeforeEach
    void setUp() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        var topology = Cleaner.build(new Cleaner.TripSerdes(TRIP, ZONE, CLEAN), new SimpleMeterRegistry());
        driver = new TopologyTestDriver(topology, props);

        var zones = driver.createInputTopic(Cleaner.ZONES, Serdes.String().serializer(), ZONE.serializer());
        zones.pipeInput("140", new Zone(140, "Manhattan", "Lenox Hill East", "Yellow Zone"));
        zones.pipeInput("236", new Zone(236, "Manhattan", "Upper East Side North", "Yellow Zone"));

        raw = driver.createInputTopic(Cleaner.RAW, Serdes.String().serializer(), TRIP.serializer());
        clean = driver.createOutputTopic(Cleaner.CLEAN, Serdes.String().deserializer(), CLEAN.deserializer());
        dlq = driver.createOutputTopic(Cleaner.DLQ, Serdes.String().deserializer(), TRIP.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void joinsZoneNamesOntoValidTrips() {
        raw.pipeInput("f:1", trip(1).build());

        CleanTrip out = clean.readValue();
        assertEquals("Lenox Hill East", out.getPickupZone().toString());
        assertEquals("Upper East Side North", out.getDropoffZone().toString());
        assertEquals("Manhattan", out.getDropoffBorough().toString());
        assertEquals(PICKUP, out.getPickupTime());
        assertEquals(12.5, out.getFareAmount());
        assertTrue(dlq.isEmpty());
    }

    @Test
    void dropsDuplicateKeys() {
        raw.pipeInput("f:1", trip(1).build());
        raw.pipeInput("f:1", trip(1).build());
        raw.pipeInput("f:2", trip(2).build());

        assertEquals(2, clean.getQueueSize());
    }

    @Test
    void sendsRejectsToDlqWithTheirRule() {
        raw.pipeInput("f:1", trip(1).setFareAmount(-12.5).build());
        raw.pipeInput("f:2", trip(2).setDropoffTime(PICKUP.minusSeconds(60)).build());
        raw.pipeInput("f:3", trip(3).setTripDistance(250.0).build());
        raw.pipeInput("f:4", trip(4).setDropoffLocationId(999).build());
        raw.pipeInput("f:5", trip(5).setPickupLocationId(null).build());

        assertRejected("f:1", "reversal");
        assertRejected("f:2", "bad_duration");
        assertRejected("f:3", "bad_distance");
        assertRejected("f:4", "unknown_zone");
        assertRejected("f:5", "unknown_zone");
        assertTrue(clean.isEmpty());
    }

    @Test
    void keepsZeroDistanceTrips() {
        raw.pipeInput("f:1", trip(1).setTripDistance(0.0).build());

        assertEquals(1, clean.getQueueSize());
    }

    private void assertRejected(String key, String rule) {
        var record = dlq.readRecord();
        assertEquals(key, record.key());
        assertEquals(rule, new String(record.headers().lastHeader("rule").value(), UTF_8));
    }

    /** A valid trip, ten minutes from zone 140 to 236. */
    private static Trip.Builder trip(long row) {
        return Trip.newBuilder()
            .setSourceFile("f")
            .setRowNumber(row)
            .setIngestedAt(PICKUP)
            .setPickupTime(PICKUP)
            .setDropoffTime(PICKUP.plusSeconds(600))
            .setTripDistance(1.2)
            .setPickupLocationId(140)
            .setDropoffLocationId(236)
            .setFareAmount(12.5)
            .setTotalAmount(18.0);
    }

    private static <T> Serde<T> serde(BinaryMessageEncoder<T> encoder, BinaryMessageDecoder<T> decoder) {
        return Serdes.serdeFrom(
            (topic, value) -> {
                try {
                    ByteBuffer buffer = encoder.encode(value);
                    byte[] bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                    return bytes;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            },
            (topic, bytes) -> {
                try {
                    return decoder.decode(bytes);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
    }
}
