package config;

/**
 * Collection of configuration constant values used throughout the application
 */
public abstract class ConfigConstants {

    public static final String COLUMN_QUAL = "col";
    public static final String COLUMN_FAMILY = "a";
    public static final String TABLE_NAME = "hhbench";


    // Available command-line option names

    public static final String OPT_ROW_COUNT = "rowCount";
    public static final String OPT_DELETE_TABLE = "deleteTable";

    public static final String OPT_ENABLE_WAL = "enableWAL";
    public static final String OPT_ENABLE_AUTOFLUSH = "autoFlush";

    public static final String OPT_SCAN_RANGE = "scanRange";
    public static final String OPT_SCAN_CACHE = "scanCache";
    public static final String OPT_SCAN_COUNT = "scanCount";

    public static final String OPT_RUN_TIMES = "runTimes";

    public static final String OPT_EXEC_TYPE = "execType";
    public static final String OPT_EXEC_TIME = "execTime";

    public static final String OPT_TEST_TYPE = "testType";

    public static final String OPT_ZK_QUORUM = "zkQuorum";
    public static final String OPT_ZK_CLIENTPORT = "zkPort";

    public static final String OPT_TOOL_TABLE = "toolTable";
    public static final String OPT_KEY_LENGTH = "keyLength";
    public static final String OPT_ROW_LENGTH = "rowLength";
    public static final String OPT_SQL_TABLE = "sqlTable";
    public static final String OPT_INDEX_NAME = "indexName";


    // Arguments used to specify the test type
    public static final String ARG_TEST_SEQREAD = "seqRead";
    public static final String ARG_TEST_SCAN = "scan";
    public static final String ARG_TEST_RANDSCAN = "randomScan";
    public static final String ARG_TEST_HCRANGESCAN = "hcRangeScan";
    public static final String ARG_TEST_GETROW = "getRow";
    public static final String ARG_TEST_BATCHWRITE = "batchWrite";
    public static final String ARG_TEST_HCWRITE = "hcWrite";
    public static final String ARG_TEST_SEQWRITE = "seqWrite";

    // Arguments used to specify the test execution type
    public static final String ARG_EXEC_TIMED = "timed";
    public static final String ARG_EXEC_COUNT = "count";
    public static final String OPT_COLUMN_FAMILY = "columnFamily";


    private ConfigConstants() {

    }
}
