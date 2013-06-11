package config;

import org.apache.commons.cli.CommandLine;

/**
 * Responsible for parsing the content of the user-defined command line arguments
 */
public abstract class CommandParser {

    /**
     * Parses the provided command line to obtain all user-defined settings
     *
     * @param line The command line content to parse
     * @return An application configuration object containing the user-defined settings
     */
    public static Config parseCommandLine(final CommandLine line) {
        final Config config = new Config();

        if (line.hasOption(ConfigConstants.OPT_KEY_LENGTH)) {
            String[] optionValue = line.getOptionValue(ConfigConstants.OPT_KEY_LENGTH).split(",");
            config.setMinKeyLength(Integer.parseInt(optionValue[0]));
            config.setMaxKeyLength(Integer.parseInt(optionValue[1]));
        }

        if (line.hasOption(ConfigConstants.OPT_ROW_LENGTH)) {
            String[] optionValue = line.getOptionValue(ConfigConstants.OPT_ROW_LENGTH).split(",");
            config.setMinRowLength(Integer.parseInt(optionValue[0]));
            config.setMaxRowLength(Integer.parseInt(optionValue[1]));
        }

        if( line.hasOption(ConfigConstants.OPT_SQL_TABLE) ) {
            config.setSqlTable(line.getOptionValue(ConfigConstants.OPT_SQL_TABLE));
        }

        if( line.hasOption(ConfigConstants.OPT_COLUMN_FAMILY) ) {
            config.setSqlTable(line.getOptionValue(ConfigConstants.OPT_COLUMN_FAMILY));
        }

        if( line.hasOption(ConfigConstants.OPT_INDEX_NAME) ) {
            config.setIndexName(line.getOptionValue(ConfigConstants.OPT_INDEX_NAME));
        }

        if( line.hasOption(ConfigConstants.OPT_ROW_COUNT) ) {
            config.setRowCount(Long.parseLong(line.getOptionValue(ConfigConstants.OPT_ROW_COUNT)));
        }

        if( line.hasOption(ConfigConstants.OPT_SCAN_RANGE) ) {
            config.setScanRange(Long.parseLong(line.getOptionValue(ConfigConstants.OPT_SCAN_RANGE)));
        }

        if( line.hasOption(ConfigConstants.OPT_SCAN_CACHE) ) {
            config.setScanCache(Integer.parseInt(line.getOptionValue(ConfigConstants.OPT_SCAN_CACHE)));
        }

        if( line.hasOption(ConfigConstants.OPT_SCAN_COUNT) ) {
            config.setScanCount(Integer.parseInt(line.getOptionValue(ConfigConstants.OPT_SCAN_COUNT)));
        }

        if( line.hasOption(ConfigConstants.OPT_TEST_TYPE) ) {
            config.setTestType(line.getOptionValue(ConfigConstants.OPT_TEST_TYPE));
        }

        if( line.hasOption(ConfigConstants.OPT_EXEC_TYPE) ) {
            config.setExecType(line.getOptionValue(ConfigConstants.OPT_EXEC_TYPE));
        }

        if( line.hasOption(ConfigConstants.OPT_RUN_TIMES) ) {
            config.setRunTimes(Integer.parseInt(line.getOptionValue(ConfigConstants.OPT_RUN_TIMES)));
        }

        if( line.hasOption(ConfigConstants.OPT_EXEC_TIME) ) {
            config.setExecutionTime(Integer.parseInt(line.getOptionValue(ConfigConstants.OPT_EXEC_TIME)));
        }

        if( line.hasOption(ConfigConstants.OPT_ENABLE_WAL) ) {
            config.setWALEnabled(true);
        }

        if( line.hasOption(ConfigConstants.OPT_ENABLE_AUTOFLUSH) ) {
            config.setAutoFlushEnabled(true);
        }

        if( line.hasOption(ConfigConstants.OPT_DELETE_TABLE) ) {
            config.setDeleteTable(true);
        }

        if( line.hasOption(ConfigConstants.OPT_TOOL_TABLE) ) {
            config.setToolTable(line.getOptionValue(ConfigConstants.OPT_TOOL_TABLE));
        }

        if( line.hasOption(ConfigConstants.OPT_ZK_QUORUM) ) {
            config.setZkQuorum(line.getOptionValue(ConfigConstants.OPT_ZK_QUORUM));
        }

        if( line.hasOption(ConfigConstants.OPT_ZK_CLIENTPORT) ) {
            config.setZkClientPort(Integer.parseInt(line.getOptionValue(ConfigConstants.OPT_ZK_CLIENTPORT)));
        }

        return config;
    }

    private CommandParser() {

    }
}
