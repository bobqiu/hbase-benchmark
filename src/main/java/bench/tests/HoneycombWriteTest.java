package bench.tests;

import static java.lang.String.format;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import util.DataProvider;
import util.HoneycombEnvironment;
import util.Utils;

import com.nearinfinity.honeycomb.mysql.HandlerProxy;

import config.Config;

/**
 * Represents a write test such that all data is handled by
 * the {@link HandlerProxy} to store Honeycomb data in HBase
 */
public final class HoneycombWriteTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(HoneycombWriteTest.class);

    private static HoneycombEnvironment hcEnv = new HoneycombEnvironment();

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final HandlerProxy proxy = hcEnv.setupEnvironment();

        long batchCount = 0;
        final long batchSize = appConfig.getBatchSize();

        proxy.createTable(DataProvider.HC_TEST_TABLE, DataProvider.HC_TABLE_SCHEMA.serialize(), 1);
        proxy.openTable(DataProvider.HC_TEST_TABLE);

        log.info(format("Writing %d rows...", appConfig.getRowCount()));

        for(int i = 1; i <= appConfig.getRowCount(); i++) {
            proxy.insertRow(Utils.generateRowValue());

            if( i % batchSize == 0 ) {
                ++batchCount;
                log.info(format("Wrote %d / %d rows", batchCount * batchSize, appConfig.getRowCount()));
            }
        }

        proxy.flush();

        proxy.closeTable();
    }
}
