package bench.tests;

import static java.lang.String.format;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import util.DataProvider;
import util.HoneycombEnvironment;
import util.Utils;

import com.nearinfinity.honeycomb.mysql.HandlerProxy;
import com.nearinfinity.honeycomb.mysql.QueryKey;

import config.Config;

/**
 *  Represents a fixed range scan over Honeycomb data using an index specified by
 *  a {@link QueryKey}
 */
public final class HoneycombRangeScanTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(HoneycombRangeScanTest.class);

    private static HoneycombEnvironment hcEnv = new HoneycombEnvironment();

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final HandlerProxy proxy = hcEnv.setupEnvironment();

        final long scanRange = appConfig.getScanRange();
        final long scanCount = appConfig.getScanCount();

        log.info(format("Performing %d table scans with potentially %d rows each...", scanCount, scanRange));

        for(int i = 1; i <= scanCount; i++) {
            log.debug(format("\nRunning scan number: %d", i));

            proxy.openTable(DataProvider.HC_TEST_TABLE);
            proxy.startIndexScan(DataProvider.HC_QUERY_KEY.serialize());

            int scannedRowCount = 0;
            for(int row = 0; row < scanRange; row++) {
                proxy.getNextRow();
                scannedRowCount++;
            }

            log.debug(format("Scan returned %d rows", scannedRowCount));
            stats.addValue(scannedRowCount);

            proxy.endScan();

            proxy.closeTable();
        }

        Utils.displayStatistics(stats, "Scanned Rows", "count");
    }
}
