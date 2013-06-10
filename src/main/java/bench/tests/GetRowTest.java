package bench.tests;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import util.DataProvider;
import util.Utils;

import com.nearinfinity.honeycomb.hbase.HBaseOperations;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;

import config.Config;

/**
 *  Represents a single row lookup by executing one {@link Get} for a specific rowkey
 */
public final class GetRowTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(GetRowTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final long lastTableId = appConfig.getRowCount() - 1;
        final byte[] rowkey = new DataRowKey(lastTableId, DataProvider.ROW_UUID).encode();

        // Look for the last row in the table
        final Get g = Utils.createGet(rowkey);

        log.info("Looking for row with rowkey: " + Utils.generateHexString(rowkey));

        final Result result = HBaseOperations.performGet(table, g);

        if( result.isEmpty() ) {
            log.info("Row could not be found");
        } else {
            log.info("Row found");
        }
    }
}
