## HBase Benchmark

HBase benchmark is a tool to test an HBase cluster. It contains a read test for Honeycomb and read/write tests for HBase directly.

The required flags for the Honeycomb test are sqlTable, columnFamily, indexName and toolTable.

## Usage

The arguments to the benchmark tool are:

```
 -autoFlush                      Enable auto flush of write buffer to
                                 write to RegionServer immediately
 -columnFamily <column family>   Column Family
 -deleteTable                    Delete the HBase table used by this tool
 -enableWAL                      Enable writing to HBase's WAL
 -execTime <time>                The minimum amount of time to execute the
                                 test type (in milliseconds)
                                 Default: 5000
 -execType <type>                The type of execution used to run the
                                 test *required*
                                 Possible types:
                                 timed: Executes the test for a minimum
                                 period of time
                                 count: Executes the test over a fixed
                                 count
 -indexName <table>              SQL table
 -keyLength <count,count>        Max row key length
                                 Default: 380
 -rowCount <count>               The total number of rows to process
                                 Default: 5000
 -rowLength <count,count>        Max row key length
                                 Default: 380
 -runTimes <count>               The number of times to execute the test
                                 type
                                 Default: 1
 -scanCache <count>              The number of rows fetched at a time by
                                 the HBase scanner for the client to
                                 process during a scan
                                 Default: 100
 -scanCount <count>              The number of times to execute a scan
 -scanRange <count>              The number of rows in the range used
                                 during random table scans
                                 Default: 100
 -sqlTable <table>               SQL table
 -testType <type>                The type of test to execute *required*
                                 Possible types:
                                 batchWrite: Writes a batch of rows to the
                                 table
                                 seqWrite: Writes one row at a time to the
                                 table in order
                                 getRow: Gets one row from the table
                                 seqRead: Reads every row from the table
                                 in order
                                 scan: Scans the entire table in order
                                 randomScan: Scans the table randomly with
                                 the specified row range
                                 hcWrite: Writes one Honeycomb row at a
                                 time to the table
                                 hcRangeScan: Scans the same range of
                                 Honeycomb rows with the specified row
                                 range
 -toolTable <table>              The name of the table used by this tool
                                 Default: hhbench
 -zkPort <port>                  The client port for Zookeeper
                                 Default: 2181
 -zkQuorum <quorum>              The quorum of Zookeeper instances
                                 Default: localhost
```                           