package bench.tests;

import static java.lang.String.format;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import util.DataProvider;
import util.Utils;

import com.nearinfinity.honeycomb.hbase.HBaseOperations;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;

import config.Config;

/**
 *  Represents a {@link Scan} over the entire set of rowkeys contained
 *  in the HBase table
 */
public final class ScanTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(ScanTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        long batchCount = 0;
        final long batchSize = appConfig.getBatchSize();
        final byte[] startRow = new DataRowKey(0, DataProvider.ROW_UUID).encode();
        final byte[] stopRow = new DataRowKey(appConfig.getRowCount(), DataProvider.ROW_UUID).encode();

        final Scan scan = Utils.createScan(startRow, stopRow);
        scan.setCaching(appConfig.getScanCache());

        final ResultScanner scanner = HBaseOperations.getScanner(table, scan);

        log.info("Performing full table scan...");

        int rowCount = 0;
        try {
            // Count the number of rows that the scanner has
            for (Result rr = null; (rr = scanner.next()) != null;) {
                ++rowCount;

                if( rowCount % batchSize == 0 ) {
                    ++batchCount;
                    log.info(format("Scanned %d / %d rows", batchCount * batchSize, appConfig.getRowCount()));
                }
            }

            stats.addValue(rowCount);
            IOUtils.closeQuietly(scanner);
        } catch (IOException e) {
            log.error("Error occurred while processing scanner results", e);
        }

        Utils.displayStatistics(stats, "Scanned Rows", "count");
    }
}
