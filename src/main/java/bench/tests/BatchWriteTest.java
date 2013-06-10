package bench.tests;

import static java.lang.String.format;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import util.Utils;

import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.hbase.HBaseOperations;

import config.Config;

/**
 * Represents a "batch" write test in that all {@link Put} objects are stored in
 * memory and written at one time to allow HBase to batch the writes
 */
public final class BatchWriteTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(BatchWriteTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        final long rowCount = appConfig.getRowCount();
        final List<Put> puts = Lists.newArrayList();
        long totalGenTime = 0;
        long startTime = 0;

        log.info(format("Preparing %d rows...", rowCount));

        // Create the specified number of rows
        for(long index = 1; index <= rowCount; index++) {
            startTime = System.currentTimeMillis();

            // Create a row with an increasing rowkey
            final Put p = Utils.createPut(Utils.generateRowKey(puts.size()), Utils.generateRowValue());
            p.setWriteToWAL(appConfig.isWALEnabled());

            puts.add(p);

            // Output a progress message
            if( index % appConfig.getBatchSize() == 0 ) {
                log.info(format("Prepared %d / %d rows for writing", puts.size(), rowCount));
            }

            totalGenTime += System.currentTimeMillis() - startTime;
        }

        log.debug(format("Total write generation time: %d ms", totalGenTime));


        startTime = System.currentTimeMillis();

        log.info("Writing all rows...");

        // Insert all of the rows with batching occurring during insertion
        HBaseOperations.performPut(table, puts);

        if( !appConfig.isAutoFlushEnabled() ) {
            HBaseOperations.performFlush(table);
        }

        stats.addValue(rowCount);
        log.debug(format("Total write insertion time: %d ms", (System.currentTimeMillis() - startTime)));

        Utils.displayStatistics(stats, "Written Rows", "count");
    }
}
