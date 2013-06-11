package util;

import com.google.common.base.Charsets;
import config.ConfigConstants;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.util.Random;

import static java.lang.String.format;

/**
 * Collection of utility methods used throughout the application
 */
public abstract class Utils {
    public static final Random RANDOM = new Random();
    private static final Logger log = Logger.getLogger(Utils.class);
    private static final int MILLIS_PER_SECOND = 1000;


    private Utils() {

    }

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
     * Displays the formatted descriptive statistics for the specified statistics
     * container
     *
     * @param stats  The stats container to display
     * @param banner The display banner used to indicate what metric is being presented
     * @param units  The units of the metric being presented
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

    public static Scan createScan(final byte[] startRowKey) {
        return new Scan(startRowKey);
    }
}
