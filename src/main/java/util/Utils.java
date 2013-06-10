package util;

import static java.lang.String.format;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;
import com.nearinfinity.honeycomb.mysql.Row;

import config.ConfigConstants;

/**
 * Collection of utility methods used throughout the application
 */
public abstract class Utils {
    private static final Logger log = Logger.getLogger(Utils.class);

    private static final Random RANDOM = new Random();
    private static final int MILLIS_PER_SECOND = 1000;


    /**
     * Generates a hex string representation of the provided byte array
     *
     * @param bytes The array of bytes to represent
     * @return The hex string representation
     */
    public static String generateHexString(final byte[] bytes) {
        final StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            sb.append(format("%02X", b));
        }

        return sb.toString();
    }

    /**
     * Generates a serialized row container object with fixed test data
     * and a random {@link UUID}
     *
     * @return The serialized row container
     */
    public static byte[] generateRowValue() {
        return new Row(DataProvider.COL_DATA, UUID.randomUUID()).serialize();
    }


    /**
     * Generates the start and stop rowkeys for a {@link Scan} range by computing a
     * random start position and a fixed stop position
     *
     * @param maxRange The size of rowkey range to generate
     * @param rowCount The maximum number of rows to scan
     * @return A container with the start and stop rowkeys
     */
    public static Pair<byte[], byte[]> generateStartAndStopRows(final long maxRange, final long rowCount) {
        long start = RANDOM.nextInt(Integer.MAX_VALUE) % rowCount;
        long stop = start + maxRange;

        log.debug(format("Generated start/stop: %d/%d", start, stop));

        return new Pair<byte[],byte[]>(generateRowKey(start), generateRowKey(stop));
      }

    public static byte[] generateRowKey(final long tableId) {
        return new DataRowKey(tableId, DataProvider.ROW_UUID).encode();
    }


    /**
     * Displays the formatted descriptive statistics for the specified statistics
     * container
     *
     * @param stats The stats container to display
     * @param banner The display banner used to indicate what metric is being presented
     * @param units The units of the metric being presented
     */
    public static void displayStatistics(final DescriptiveStatistics stats, final String banner, final String units) {
        System.out.println("\n*************************");
        System.out.println(String.format("%s Statistics", banner));
        System.out.println("Units: " + units);
        System.out.println("**************************");
        System.out.println("Total samples: " + stats.getN());
        System.out.println("Min: " + stats.getMin());
        System.out.println("Max: " + stats.getMax());
        System.out.println("Sum: " + stats.getSum());
        System.out.println("Mean: " + stats.getMean());
        System.out.println("Median: " + stats.getPercentile(50));
        System.out.println("Standard Deviation: " + stats.getStandardDeviation());
        System.out.println("Variance: " + stats.getVariance());
    }

    public static double convertMillisToSeconds(final double millis) {
        return millis / MILLIS_PER_SECOND;
    }

    public static Get createGet(final byte[] rowkey) {
        return new Get(rowkey)
            .addColumn(ConfigConstants.COLUMN_FAMILY.getBytes(Charsets.UTF_8),
                       ConfigConstants.COLUMN_QUAL.getBytes(Charsets.UTF_8));
    }

    public static Put createPut(final byte[] rowKey, final byte[] value) {
        return new Put(rowKey)
            .add(ConfigConstants.COLUMN_FAMILY.getBytes(Charsets.UTF_8),
                 ConfigConstants.COLUMN_QUAL.getBytes(Charsets.UTF_8),
                 value);
    }

    public static Scan createScan(final byte[] startRowKey, final byte[] stopRowKey) {
        return new Scan(startRowKey, stopRowKey)
            .addColumn(ConfigConstants.COLUMN_FAMILY.getBytes(Charsets.UTF_8),
                       ConfigConstants.COLUMN_QUAL.getBytes(Charsets.UTF_8));
    }

    private Utils() {

    }
}
