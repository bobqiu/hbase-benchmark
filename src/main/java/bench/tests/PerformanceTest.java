package bench.tests;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;

import config.Config;

/**
 * Any entity that wishes to execute a performance test must adhere to this interface
 */
public interface PerformanceTest {

    /**
     * Runs the logic specified by this test to perform a performance test for benchmarking
     * purposes
     *
     * @param table The connection to the HBase table
     * @param appConfig The application specific configuration
     * @param stats The statistics container used to collect a metric from the test
     */
    public void runTest(final HTableInterface table, final Config appConfig, final DescriptiveStatistics stats);
}
