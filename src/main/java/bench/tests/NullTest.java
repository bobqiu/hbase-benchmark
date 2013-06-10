package bench.tests;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import config.Config;

/**
 * Null Object implementation used as the default test when an improper test
 * has been specified
 */
public final class NullTest implements PerformanceTest {
    private static final Logger log = Logger.getLogger(NullTest.class);

    @Override
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats) {
        log.warn("Unknown test specified.  Nothing to run.");
    }
}
