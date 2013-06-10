package config;

import static java.lang.String.format;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * Responsible for defining and storing all of the available options exposed
 * through the Command Line Interface
 */
@SuppressWarnings("static-access")
public abstract class CommandOptions {

    private static final String ARG_NAME_TABLE = "table";
    private static final String ARG_NAME_PORT = "port";
    private static final String ARG_NAME_QUORUM = "quorum";
    private static final String ARG_NAME_TIME = "time";
    private static final String ARG_NAME_TYPE = "type";
    private static final String ARG_NAME_COUNT = "count";

    public static final Options CMD_LINE_OPTS = new Options();

    private static final String DEFAULT = "\nDefault:";

    static {
        final Option rowCount = OptionBuilder.withArgName(ARG_NAME_COUNT)
                                             .hasArg()
                                             .withDescription(format("The total number of rows to process %s %d", DEFAULT, Config.DEFAULT_ROW_COUNT))
                                             .create(ConfigConstants.OPT_ROW_COUNT);

        final Option writeWAL = OptionBuilder.hasArg(false)
                                             .withDescription("Enable writing to HBase's WAL")
                                             .create(ConfigConstants.OPT_ENABLE_WAL);

        final Option autoFlush = OptionBuilder.hasArg(false)
                                              .withDescription("Enable auto flush of write buffer to write to RegionServer immediately")
                                              .create(ConfigConstants.OPT_ENABLE_AUTOFLUSH);

        final Option deleteTable = OptionBuilder.hasArg(false)
                                                .withDescription("Delete the HBase table used by this tool")
                                                .create(ConfigConstants.OPT_DELETE_TABLE);

        final Option scanRange = OptionBuilder.withArgName(ARG_NAME_COUNT)
                                              .hasArg(true)
                                              .withDescription(format("The number of rows in the range used during random table scans %s %d", DEFAULT, Config.DEFAULT_SCAN_RANGE_ROWS))
                                              .create(ConfigConstants.OPT_SCAN_RANGE);

        final Option scanCache = OptionBuilder.withArgName(ARG_NAME_COUNT)
                                              .hasArg(true)
                                              .withDescription(format("The number of rows fetched at a time by the HBase scanner for the client to process during a scan %s %d", DEFAULT, Config.DEFAULT_SCAN_CACHE_ROWS))
                                              .create(ConfigConstants.OPT_SCAN_CACHE);

        final Option scanCount = OptionBuilder.withArgName(ARG_NAME_COUNT)
                                              .hasArg(true)
                                              .withDescription("The number of times to execute a scan")
                                              .create(ConfigConstants.OPT_SCAN_COUNT);

        final Option zkQuorum = OptionBuilder.withArgName(ARG_NAME_QUORUM)
                                             .hasArg(true)
                                             .withDescription(format("The quorum of Zookeeper instances %s %s", DEFAULT, Config.DEFAULT_ZOOKEEPER_QUORUM))
                                             .create(ConfigConstants.OPT_ZK_QUORUM);

        final Option zkClientPort = OptionBuilder.withArgName(ARG_NAME_PORT)
                                                 .hasArg(true)
                                                 .withDescription(format("The client port for Zookeeper %s %d", DEFAULT, Config.DEFAULT_ZOOKEEPER_CLIENTPORT))
                                                 .create(ConfigConstants.OPT_ZK_CLIENTPORT);

        final Option toolTable = OptionBuilder.withArgName(ARG_NAME_TABLE)
                                              .hasArg(true)
                                              .withDescription(format("The name of the table used by this tool %s %s",  DEFAULT, ConfigConstants.TABLE_NAME))
                                              .create(ConfigConstants.OPT_TOOL_TABLE);


        final Option testType = OptionBuilder.withArgName(ARG_NAME_TYPE)
                                             .isRequired(true)
                                             .hasArg(true)
                                             .withDescription("The type of test to execute \nPossible types:\n" +
                                                             ConfigConstants.ARG_TEST_BATCHWRITE + ": Writes a batch of rows to the table\n" +
                                                             ConfigConstants.ARG_TEST_SEQWRITE + ": Writes one row at a time to the table in order\n" +
                                                             ConfigConstants.ARG_TEST_GETROW + ": Gets one row from the table\n" +
                                                             ConfigConstants.ARG_TEST_SEQREAD + ": Reads every row from the table in order\n" +
                                                             ConfigConstants.ARG_TEST_SCAN + ": Scans the entire table in order\n" +
                                                             ConfigConstants.ARG_TEST_RANDSCAN + ": Scans the table randomly with the specified row range\n" +
                                                             ConfigConstants.ARG_TEST_HCWRITE + ": Writes one Honeycomb row at a time to the table\n" +
                                                             ConfigConstants.ARG_TEST_HCRANGESCAN + ": Scans the same range of Honeycomb rows with the specified row range")
                                             .create(ConfigConstants.OPT_TEST_TYPE);

        final Option execType = OptionBuilder.withArgName(ARG_NAME_TYPE)
                                             .hasArg(true)
                                             .isRequired(true)
                                             .withDescription("The type of execution used to run the test \nPossible types:\n" +
                                                             ConfigConstants.ARG_EXEC_TIMED + ": Executes the test for a minimum period of time\n" +
                                                             ConfigConstants.ARG_EXEC_COUNT + ": Executes the test over a fixed count")
                                             .create(ConfigConstants.OPT_EXEC_TYPE);

        final Option runTimes = OptionBuilder.withArgName(ARG_NAME_COUNT)
                                             .hasArg(true)
                                             .withDescription(format("The number of times to execute the test type %s %d", DEFAULT, Config.DEFAULT_RUN_TIMES))
                                             .create(ConfigConstants.OPT_RUN_TIMES);

        final Option execTime = OptionBuilder.withArgName(ARG_NAME_TIME)
                                             .hasArg(true)
                                             .withDescription(format("The minimum amount of time to execute the test type (in milliseconds) %s %d", DEFAULT, Config.DEFAULT_EXECUTION_TIME_MS))
                                             .create(ConfigConstants.OPT_EXEC_TIME);

        CMD_LINE_OPTS.addOption(rowCount);
        CMD_LINE_OPTS.addOption(writeWAL);
        CMD_LINE_OPTS.addOption(autoFlush);
        CMD_LINE_OPTS.addOption(scanRange);
        CMD_LINE_OPTS.addOption(scanCache);
        CMD_LINE_OPTS.addOption(scanCount);
        CMD_LINE_OPTS.addOption(deleteTable);
        CMD_LINE_OPTS.addOption(runTimes);
        CMD_LINE_OPTS.addOption(execTime);
        CMD_LINE_OPTS.addOption(zkQuorum);
        CMD_LINE_OPTS.addOption(zkClientPort);
        CMD_LINE_OPTS.addOption(toolTable);
        CMD_LINE_OPTS.addOption(testType);
        CMD_LINE_OPTS.addOption(execType);
    }

    private CommandOptions () {

    }
}

