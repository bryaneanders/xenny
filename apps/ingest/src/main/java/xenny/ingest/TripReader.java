package xenny.ingest;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import xenny.trips.Trip;

/** Reads a TLC trip Parquet file as Trip records. */
final class TripReader implements Closeable {

    /** Thrown when the file can't be read as a trip file. */
    static final class BadFileException extends Exception {
        BadFileException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    // TLC files record New York wall-clock times.
    private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

    private final ParquetReader<GenericRecord> reader;
    private final String sourceFile;
    private final Instant ingestedAt;
    // Lower-cased column name -> position, since column casing varies between years.
    private Map<String, Integer> columns;
    private long rowNumber;

    TripReader(Path file, String sourceFile, Instant ingestedAt) throws BadFileException {
        this.sourceFile = sourceFile;
        this.ingestedAt = ingestedAt;
        try {
            reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(file), new PlainParquetConfiguration())
                .build();
        } catch (Exception e) {
            throw new BadFileException(e);
        }
    }

    /** Returns the next trip, or null at the end of the file. */
    Trip next() throws BadFileException {
        GenericRecord row;
        try {
            row = reader.read();
            if (row == null) {
                return null;
            }
            if (columns == null) {
                columns = index(row.getSchema());
            }
            rowNumber++;
            return Trip.newBuilder()
                .setSourceFile(sourceFile)
                .setRowNumber(rowNumber)
                .setIngestedAt(ingestedAt)
                .setVendorId(toInt(get(row, "vendorid")))
                .setPickupTime(toInstant(get(row, "tpep_pickup_datetime")))
                .setDropoffTime(toInstant(get(row, "tpep_dropoff_datetime")))
                .setPassengerCount(toLong(get(row, "passenger_count")))
                .setTripDistance(toDouble(get(row, "trip_distance")))
                .setRatecodeId(toLong(get(row, "ratecodeid")))
                .setStoreAndFwdFlag(toStr(get(row, "store_and_fwd_flag")))
                .setPickupLocationId(toInt(get(row, "pulocationid")))
                .setDropoffLocationId(toInt(get(row, "dolocationid")))
                .setPaymentType(toLong(get(row, "payment_type")))
                .setFareAmount(toDouble(get(row, "fare_amount")))
                .setExtra(toDouble(get(row, "extra")))
                .setMtaTax(toDouble(get(row, "mta_tax")))
                .setTipAmount(toDouble(get(row, "tip_amount")))
                .setTollsAmount(toDouble(get(row, "tolls_amount")))
                .setImprovementSurcharge(toDouble(get(row, "improvement_surcharge")))
                .setTotalAmount(toDouble(get(row, "total_amount")))
                .setCongestionSurcharge(toDouble(get(row, "congestion_surcharge")))
                .setAirportFee(toDouble(get(row, "airport_fee")))
                .setCbdCongestionFee(toDouble(get(row, "cbd_congestion_fee")))
                .setRequestSource(toStr(get(row, "request_source")))
                .build();
        } catch (Exception e) {
            throw new BadFileException(e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    private static Map<String, Integer> index(Schema schema) {
        Map<String, Integer> index = new HashMap<>();
        for (Schema.Field f : schema.getFields()) {
            index.put(f.name().toLowerCase(Locale.ROOT), f.pos());
        }
        return index;
    }

    // Null when the column isn't in this file (older files lack newer columns).
    private Object get(GenericRecord row, String column) {
        Integer pos = columns.get(column);
        return pos == null ? null : row.get(pos);
    }

    private static Integer toInt(Object v) {
        return v == null ? null : ((Number) v).intValue();
    }

    private static Long toLong(Object v) {
        return v == null ? null : ((Number) v).longValue();
    }

    private static Double toDouble(Object v) {
        return v == null ? null : ((Number) v).doubleValue();
    }

    private static String toStr(Object v) {
        return v == null ? null : v.toString();
    }

    private static Instant toInstant(Object v) {
        if (v == null) {
            return null;
        }
        LocalDateTime local = v instanceof LocalDateTime ldt ? ldt : fromMicros((Long) v);
        return local.atZone(NEW_YORK).toInstant();
    }

    private static LocalDateTime fromMicros(long micros) {
        return LocalDateTime.ofEpochSecond(
            Math.floorDiv(micros, 1_000_000), (int) Math.floorMod(micros, 1_000_000) * 1_000, ZoneOffset.UTC);
    }
}
