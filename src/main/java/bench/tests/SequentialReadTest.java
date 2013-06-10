package bench.tests;

import static java.lang.String.format;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import util.DataProvider;
import util.Utils;

import com.nearinfinity.honeycomb.hbase.HBaseOperations;
import com.nearinfinity.honeycomb.hbase.rowkey.DataRowKey;

import config.Config;

/**
 * Represents a "sequential" read test in that each row corresponds to one {@link Get}
 * object that is used to fetch a row from the HBase table
 */
public final class SequentialReadTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(SequentialReadTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final long batchSize = appConfig.getBatchSize();
        long batchCount = 0;

        log.info("Performing sequential read...");

        for(int i = 1; i <= appConfig.getRowCount(); i++) {
            final Get g = Utils.createGet(new DataRowKey(i, DataProvider.ROW_UUID).encode());

            HBaseOperations.performGet(table, g);

            if( i % batchSize == 0 ) {
                ++batchCount;
                log.info(format("Read %d / %d rows", batchCount * batchSize, appConfig.getRowCount()));
            }
        }
    }
}
