package bench.tests;

import static java.lang.String.format;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import util.Utils;

import com.nearinfinity.honeycomb.hbase.HBaseOperations;

import config.Config;

/**
 *  Represents a {@link Scan} over a random, fixed range of rowkeys contained
 *  in the HBase table
 */
public final class RandomScanTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(RandomScanTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final long scanRange = appConfig.getScanRange();
        final long scanCount = appConfig.getScanCount();

        long totalCreationTime = 0;
        long totalScanTime = 0;

        log.info(format("Performing %d random table scans with potentially %d rows each...", scanCount, scanRange));

        // Run scans for the specified number of times
        for(int i = 1; i <= scanCount; i++) {
            log.debug(format("\nRunning scan number: %d", i));

            long startTime = System.currentTimeMillis();
            final Pair<byte[], byte[]> startAndStopRow = Utils.generateStartAndStopRows(scanRange, appConfig.getRowCount());

            final Scan scan = Utils.createScan(startAndStopRow.getFirst(), startAndStopRow.getSecond());
            scan.setCaching(appConfig.getScanCache());

            log.debug(format("Scan range [%s, %s]",
                    Utils.generateHexString(startAndStopRow.getFirst()),
                    Utils.generateHexString(startAndStopRow.getSecond())));

            totalCreationTime += System.currentTimeMillis() - startTime;


            startTime = System.currentTimeMillis();
            ResultScanner scanner = HBaseOperations.getScanner(table, scan);
            int resultCount = 0;

            try {
                // Count the number of rows that the scanner has
                for (Result rr = null; (rr = scanner.next()) != null;) {
                    resultCount++;
                }
            } catch (IOException e) {
                log.error("Error occurred while processing scanner results", e);
            }

            log.debug(format("Scan returned %d rows", resultCount));
            stats.addValue(resultCount);

            IOUtils.closeQuietly(scanner);

            totalScanTime += System.currentTimeMillis() - startTime;
        }

        log.debug(format("Total scan creation time: ms", totalCreationTime));
        log.debug(format("Total scan execution time: ms", totalScanTime));

        Utils.displayStatistics(stats, "Scanned Rows", "count");
    }
}
