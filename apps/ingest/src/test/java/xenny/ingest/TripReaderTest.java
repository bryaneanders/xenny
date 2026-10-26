package xenny.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import xenny.trips.Trip;

class TripReaderTest {

    // Three real rows from yellow_tripdata_2026-08.parquet; the third has nulls.
    private static final Path SAMPLE = Path.of("src/test/resources/trips-sample.parquet");
    private static final Instant INGESTED_AT = Instant.parse("2026-10-25T00:00:00Z");

    @Test
    void mapsRowsAndConvertsNewYorkTimesToUtc() throws Exception {
        try (var reader = new TripReader(SAMPLE, "trips/sample.parquet", INGESTED_AT)) {
            Trip first = reader.next();
            assertEquals("trips/sample.parquet", first.getSourceFile().toString());
            assertEquals(1, first.getRowNumber());
            assertEquals(INGESTED_AT, first.getIngestedAt());
            assertEquals(1, first.getVendorId());
            // 00:03:04 EDT is 04:03:04 UTC.
            assertEquals(Instant.parse("2026-08-01T04:03:04Z"), first.getPickupTime());
            assertEquals(1L, first.getPassengerCount());
            assertEquals(140, first.getPickupLocationId());
            assertEquals(6.5, first.getFareAmount());
            assertEquals(0.0, first.getAirportFee());

            assertEquals(2, reader.next().getRowNumber());

            Trip third = reader.next();
            assertEquals(3, third.getRowNumber());
            assertNull(third.getPassengerCount());
            assertNull(third.getAirportFee());
            assertEquals("HV0003", third.getRequestSource().toString());

            assertNull(reader.next());
        }
    }

    @Test
    void rejectsFilesThatArentParquet(@TempDir Path dir) throws Exception {
        Path notParquet = Files.writeString(dir.resolve("bad.parquet"), "not a parquet file");
        assertThrows(TripReader.BadFileException.class, () -> {
            try (var reader = new TripReader(notParquet, "trips/bad.parquet", INGESTED_AT)) {
                reader.next();
            }
        });
    }
}
