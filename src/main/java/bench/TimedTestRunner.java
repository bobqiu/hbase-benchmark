package bench;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.client.HTableInterface;

import bench.tests.PerformanceTest;
import config.Config;


/**
 * Responsible for running a {@link PerformanceTest} in a separate {@link Thread}
 * until being signaled to complete
 */
public final class TimedTestRunner implements Runnable {
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private final PerformanceTest test;
    private final HTableInterface table;
    private final Config appConfig;
    private final DescriptiveStatistics testStats;
    private final CountDownLatch doneLatch;

    /**
     *
     *
     * @param tableConn The connection used to communicate with the HBase table, not null
     * @param perfTest The test to run, not null
     * @param config The application configuration for the test to use, not null
     * @param stats The statistics container used to collect a metric from the test, not null
     * @param latch The latch used to signal the completion of the test, not null
     */
    public TimedTestRunner(final HTableInterface tableConn, final PerformanceTest perfTest, final Config config,
            final DescriptiveStatistics stats, final CountDownLatch latch) {
        checkNotNull(tableConn);
        checkNotNull(perfTest);
        checkNotNull(config);
        checkNotNull(stats);
        checkNotNull(latch);

        table = tableConn;
        test = perfTest;
        appConfig = config;
        testStats = stats;
        doneLatch = latch;
    }


    @Override
    public void run() {
        try {
            // Run the test until instructed to finish
            while(!finished.get()) {
                test.runTest(table, appConfig, testStats);
            }
        } finally {
            // Signal that the thread is finished running test executions
            doneLatch.countDown();
        }
    }

    /**
     * Signals the runner to finish running new executions of the test
     */
    public void signalFinish() {
        finished.set(true);
    }
}
