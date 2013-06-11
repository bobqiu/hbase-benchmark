package bench.tests;

import com.nearinfinity.honeycomb.hbase.HBaseOperations;
import config.Config;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import util.DataProvider;
import util.Utils;

import java.io.IOException;

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
        // Run scans for the specified number of times
        for (int i = 1; i <= scanCount; i++) {
            int i1 = Utils.RANDOM.nextInt(DataProvider.QUERY_KEYS.length);
            byte[] startRowKey = DataProvider.QUERY_KEYS[i1];
            final Scan scan = Utils.createScan(startRowKey);
            scan.setCaching(appConfig.getScanCache());
            ResultScanner scanner = HBaseOperations.getScanner(table, scan);

            int resultCount = 0;

            try {
                for (int index = 0; index < scanRange; index++) {
                    Result rr = scanner.next();
                    if (rr == null) {
                        break;
                    }
                    resultCount++;
                }

            } catch (IOException e) {
                log.error("Error occurred while processing scanner results", e);
            }

            stats.addValue(resultCount);

            IOUtils.closeQuietly(scanner);
        }
    }
}
