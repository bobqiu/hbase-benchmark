package bench.tests;

import static java.lang.String.format;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import util.Utils;

import com.google.common.collect.ImmutableList;
import com.nearinfinity.honeycomb.hbase.HBaseOperations;

import config.Config;

/**
 * Represents a "sequential" write test in that one new row corresponds to one {@link Put}
 * object that is written one at a time to the HBase table
 */
public final class SequentialWriteTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(SequentialWriteTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final long rowCount = appConfig.getRowCount();
        final long batchSize = appConfig.getBatchSize();
        long batchCount = 0;
        long rowKeyCount = 0;

        log.info(format("Writing %d rows...", rowCount));

        for(long index = 1; index <= rowCount; index++) {

            final Put p = Utils.createPut(Utils.generateRowKey(rowKeyCount), Utils.generateRowValue());
            p.setWriteToWAL(appConfig.isWALEnabled());

            HBaseOperations.performPut(table, ImmutableList.<Put>of(p));
            rowKeyCount++;

            if( index % batchSize == 0 ) {
                ++batchCount;
                log.info(format("Wrote %d / %d rows", batchCount * batchSize, rowCount));
            }
        }

        if( !appConfig.isAutoFlushEnabled() ) {
            HBaseOperations.performFlush(table);
        }
    }
}
