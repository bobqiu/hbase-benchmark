package config;

import com.google.common.base.Objects;

/**
 * Simple POJO bean that holds the application configuration settings
 */
public final class Config {

    public static final long DEFAULT_ROW_COUNT = 5000;
    public static final long DEFAULT_SCAN_RANGE_ROWS = 100;

    public static final int DEFAULT_SCAN_CACHE_ROWS = 100;
    public static final int DEFAULT_RUN_TIMES = 1;
    public static final int DEFAULT_EXECUTION_TIME_MS = 5000;

    public static final String DEFAULT_ZOOKEEPER_QUORUM = "localhost";
    public static final int DEFAULT_ZOOKEEPER_CLIENTPORT = 2181;

    private String testType = "";
    private String execType = "";
    private String toolTable = ConfigConstants.TABLE_NAME;
    private String zkQuorum = DEFAULT_ZOOKEEPER_QUORUM;
    private int zkClientPort = DEFAULT_ZOOKEEPER_CLIENTPORT;
    private long rowCount = DEFAULT_ROW_COUNT;
    private long scanRange = DEFAULT_SCAN_RANGE_ROWS;
    private int scanCache = DEFAULT_SCAN_CACHE_ROWS;
    private int scanCount = -1;
    private int runTimes = DEFAULT_RUN_TIMES;
    private int executionTime = DEFAULT_EXECUTION_TIME_MS;
    private boolean WALEnabled = false;
    private boolean autoFlushEnabled = false;
    private boolean deleteTable = false;

    public boolean isAutoFlushEnabled() {
        return autoFlushEnabled;
    }

    public void setAutoFlushEnabled(boolean autoFlushEnabled) {
        this.autoFlushEnabled = autoFlushEnabled;
    }

    public String getExecType() {
        return execType;
    }

    public void setExecType(String execType) {
        this.execType = execType;
    }

    public int getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(int executionTime) {
        this.executionTime = executionTime;
    }

    public String getToolTable() {
        return toolTable;
    }

    public void setToolTable(String toolTable) {
        this.toolTable = toolTable;
    }

    public int getZkClientPort() {
        return zkClientPort;
    }

    public void setZkClientPort(int zkClientPort) {
        this.zkClientPort = zkClientPort;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    public int getScanCount() {
        if( scanCount == -1 ) {
            return (int) (rowCount / scanRange);
        }
        return scanCount;
    }

    public void setScanCount(int scanCount) {
        this.scanCount = scanCount;
    }

    public int getScanCache() {
        return scanCache;
    }

    public void setScanCache(int scanCache) {
        this.scanCache = scanCache;
    }

    public long getBatchSize() {
        return rowCount / 10L;
    }

    public int getRunTimes() {
        return runTimes;
    }

    public void setRunTimes(int runTimes) {
        this.runTimes = runTimes;
    }

    public String getTestType() {
        return testType;
    }

    public void setTestType(String testType) {
        this.testType = testType;
    }

    public boolean isDeleteTable() {
        return deleteTable;
    }

    public void setDeleteTable(boolean deleteTable) {
        this.deleteTable = deleteTable;
    }

    public long getScanRange() {
        return scanRange;
    }

    public void setScanRange(long scanRange) {
        this.scanRange = scanRange;
    }

    public boolean isWALEnabled() {
        return WALEnabled;
    }

    public void setWALEnabled(boolean wALEnabled) {
        WALEnabled = wALEnabled;
    }

    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this.getClass())
                .add("Test type", testType)
                .add("Execution type", execType)
                .add("Execution times", runTimes)
                .add("Total row count", rowCount)
                .add("Batch (interval) size", getBatchSize())
                .add("Write Ahead Log enabled", WALEnabled)
                .add("Auto flush writes enabled", autoFlushEnabled)
                .add("Scan range (rows)", scanRange)
                .add("Scan cache (rows)", scanCache)
                .add("Delete table enabled", deleteTable)
                .add("Tool table", toolTable)
                .add("Zookeeper quorum", zkQuorum)
                .add("Zookeeper client port", zkClientPort)
                .toString();
    }
}
