package bench;

import bench.tests.*;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import config.CommandOptions;
import config.CommandParser;
import config.Config;
import config.ConfigConstants;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.log4j.Logger;
import util.DataProvider;
import util.HoneycombQueryGenerator;
import util.Utils;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * The main entry point of the benchmark tool responsible for setting up application
 * configuration and execution of a benchmark test
 */
public final class BenchmarkDriver {
    private static final Logger log = Logger.getLogger(BenchmarkDriver.class);

    private final Configuration config;
    private final Config appConfig;
    private HTableInterface tableConnection;

    private static final ImmutableMap<String, PerformanceTest> TEST_LOOKUP =
            new ImmutableMap.Builder<String, PerformanceTest>()
                .put(ConfigConstants.ARG_TEST_BATCHWRITE, new BatchWriteTest())
                .put(ConfigConstants.ARG_TEST_HCWRITE, new HoneycombWriteTest())
                .put(ConfigConstants.ARG_TEST_SEQWRITE, new SequentialWriteTest())
                .put(ConfigConstants.ARG_TEST_GETROW, new GetRowTest())
                .put(ConfigConstants.ARG_TEST_SEQREAD, new SequentialReadTest())
                .put(ConfigConstants.ARG_TEST_SCAN, new ScanTest())
                .put(ConfigConstants.ARG_TEST_RANDSCAN, new RandomScanTest())
                .put(ConfigConstants.ARG_TEST_HCRANGESCAN, new HoneycombRangeScanTest())
                .build();

    private static final PerformanceTest NULL_TEST = new NullTest();


    public static void main(final String[] args) {
        final CommandLineParser parser = new PosixParser();

        CommandLine line = null;
        try {
            line = parser.parse(CommandOptions.CMD_LINE_OPTS, args);
        } catch (ParseException pe) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java -jar <jarpath>", CommandOptions.CMD_LINE_OPTS);

            System.exit(1);
        }

        final BenchmarkDriver driver = new BenchmarkDriver(HBaseConfiguration.create(), CommandParser.parseCommandLine(line));
        driver.setupToolTable();
        driver.runBenchmark();
    }

    /**
     *
     *
     * @param conf The HBase configuration used to run this application, not null
     * @param app The user-defined configuration used throughout the application, not null
     */
    BenchmarkDriver(final Configuration conf, final Config app) {
        checkNotNull(conf, "The provided Hadoop/HBase configuration is invalid");
        checkNotNull(app, "The provided application configuration is invalid");

        config = conf;
        appConfig = app;

        config.set("hbase.zookeeper.quorum", appConfig.getZkQuorum());
        config.set("hbase.zookeeper.property.clientPort", String.valueOf(appConfig.getZkClientPort()));
    }

    /**
     * Performs the operations necessary to setup the required HBase table used by this tool
     */
    public void setupToolTable() {
        HBaseAdmin admin = null;

        DataProvider.QUERY_KEYS = HoneycombQueryGenerator.generate(appConfig);
        try {
            admin = new HBaseAdmin(config);

            // If a table delete has been requested, attempt to delete it first
            if( appConfig.isDeleteTable() ) {
                deleteTable(admin);
            }

            createTable(admin);
            connectToTable();
        } catch (MasterNotRunningException e) {
            log.error("The HMaster does not appear to be running", e);
        } catch (ZooKeeperConnectionException e) {
            log.error("Unable to connect to ZooKeeper", e);
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }


    /**
     * Attempts to create the table used by this tool with the fixed configuration details
     *
     * @param admin The configured administration used to perform this operation
     */
    private void createTable(final HBaseAdmin admin) {
        final String tableName = appConfig.getToolTable();

        try {
            if( !admin.tableExists(tableName) ) {
                HTableDescriptor tableDesc = new HTableDescriptor(tableName.getBytes(Charsets.UTF_8));

                HColumnDescriptor colDesc = new HColumnDescriptor(ConfigConstants.COLUMN_FAMILY);
                colDesc.setBlockCacheEnabled(true).setBlocksize(65536)
                        .setBloomFilterType(BloomType.ROW)
                        .setCompressionType(Algorithm.SNAPPY)
                        .setDataBlockEncoding(DataBlockEncoding.PREFIX)
                        .setMaxVersions(1);

                tableDesc.addFamily(colDesc);

                admin.createTable(tableDesc);
                log.info("Created table: " + tableName);
            } else {
                log.debug("Table already exists, creation skipped");
            }
        } catch (IOException e) {
            log.error("Error occurred during table creation", e);
        }
    }


    /**
     * Attempts to delete the table used by this tool
     *
     * @param admin The configured administration used to perform this operation
     */
    private void deleteTable(final HBaseAdmin admin) {
        final String tableName = appConfig.getToolTable();

        try {
            if( admin.tableExists(tableName) ) {
                if( admin.isTableAvailable(tableName) ) {
                    admin.disableTable(tableName);
                }

                admin.deleteTable(tableName);
                log.info("Deleted table: " + tableName);
            } else {
                log.debug("Could not find table to delete");
            }
        } catch (IOException e) {
            log.error("Error occurred during table deletion", e);
        }
    }

    /**
     * Creates and configures a new connection to the HBase table
     */
    private void connectToTable() {
        try {
            tableConnection = new HTable(config, appConfig.getToolTable());
            tableConnection.setAutoFlush(appConfig.isAutoFlushEnabled());
        } catch (IOException e) {
            log.error("Error occurred during table connection", e);
        }
    }

    /**
     * Run the benchmark test with the user-defined configuration options
     */
    public void runBenchmark() {
        checkState(tableConnection != null, "Connection to table does not exist");

        final DescriptiveStatistics totalExecutionTimeStats = new DescriptiveStatistics();
        final DescriptiveStatistics testStats = new DescriptiveStatistics();
        final DescriptiveStatistics totalTestStats = new DescriptiveStatistics();

        log.info("Execution Configuration: " + appConfig);

        final PerformanceTest test = fetchTest();

        log.info("Running test: " + appConfig.getTestType());

        // Execute the test a total number of times with the specified number of runs
        for(int testRun = 1; testRun <= appConfig.getRunTimes(); testRun++) {

            log.info("Test execution number: " + testRun);

            final long startTime = System.currentTimeMillis();

            if(appConfig.getExecType().equals(ConfigConstants.ARG_EXEC_TIMED)) {
                runTimedTest(test, testStats);
            } else if( appConfig.getExecType().equals(ConfigConstants.ARG_EXEC_COUNT) ) {
                test.runTest(tableConnection, appConfig, testStats);
            }

            final long runTime = System.currentTimeMillis() - startTime;

            log.info(format("Test execution time: %d ms", runTime));
            totalExecutionTimeStats.addValue(runTime);

            // Output test run statistical data, if available
            if( testStats.getN() > 0 ) {
                log.info(format("Units per second: %.2f", testStats.getSum() / Utils.convertMillisToSeconds(runTime)));

                // Add this run's data to the cumulative test stats
                final double[] testValues = testStats.getValues();
                for(int index = 0; index < testValues.length; index++) {
                    totalTestStats.addValue(testValues[index]);
                }

                // Clear the stats from this test run to reuse the stat container
                testStats.clear();
            }
        }
            displayCumulativeStats(totalExecutionTimeStats, totalTestStats);
    }

    /**
     * Look for the test to run based on the type provided
     *
     * @return The performance test to execute
     */
    private PerformanceTest fetchTest() {
        final PerformanceTest test = TEST_LOOKUP.get(appConfig.getTestType());

        // If no test was found, use the Null Object test
        if( test == null ) {
            return NULL_TEST;
        }

        return test;
    }

    /**
     * Displays the cumulative statistics from the execution(s) of the performance test, if available
     *
     * @param totalExecutionStats The cumulative execution time
     * @param totalTestStats The cumulative statistics metric collected from the test
     */
    private void displayCumulativeStats(final DescriptiveStatistics totalExecutionStats, final DescriptiveStatistics totalTestStats) {
        Utils.displayStatistics(totalExecutionStats, "Execution Time", "milliseconds");

        // Output cumulative test statistics, if available
        if( totalTestStats.getN() > 0 && totalExecutionStats.getSum() > 0 ) {
            // Since the test can provide stats on any metric, use an arbitrary unit identifier
            Utils.displayStatistics(totalTestStats, "Units", "");
            log.info(format("Average units per second: %.2f", totalTestStats.getSum() / Utils.convertMillisToSeconds(totalExecutionStats.getSum())));
        }
    }

    /**
     * Runs the provided test multiple times until the minimum execution time
     * limit has been reached
     *
     * @param test The test to run
     * @param testStats The stats container used to collect a metric from the test
     */
    private void runTimedTest(final PerformanceTest test, final DescriptiveStatistics testStats) {
        final CountDownLatch doneLatch = new CountDownLatch(1);
        final TimedTestRunner testRunner = new TimedTestRunner(tableConnection, test, appConfig, testStats, doneLatch);
        final Thread t = new Thread(testRunner, format("%s Thread", testRunner.getClass().getSimpleName()));
        t.start();

        // Let the test runner execute for the specified period of time before termination
        try {
            Thread.sleep(appConfig.getExecutionTime());
        } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();

            log.error("Test execution period interrupted", e);
        }

        log.info("Minimum execution time exceeded, requesting clean termination of test");
        testRunner.signalFinish();
        try {
            // Wait for the test runner thread to finish
            doneLatch.await();
        } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();

            log.error("Waiting for test to finish was interrupted", e);
        } finally {
            log.info("Test has been terminated");
        }
    }
}