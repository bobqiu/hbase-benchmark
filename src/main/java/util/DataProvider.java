package util;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import bench.tests.GetRowTest;
import bench.tests.PerformanceTest;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.ColumnType;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.ColumnSchema;
import com.nearinfinity.honeycomb.mysql.schema.IndexSchema;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;

/**
 * Container class used to provide test data to {@link PerformanceTest} implementations
 */
public abstract class DataProvider {

    private static final String HC_INDEX_NAME = "idxName";

    private static final String[] COL_NAMES = { "firstName", "lastName",
        "address", "zip", "state", "country", "phone", "salary", "fk"};

    /**
     * Collection of column names and test data used by the tests
     */
    public static final Map<String, ByteBuffer> COL_DATA = new ImmutableMap.Builder<String, ByteBuffer>()
            .put(COL_NAMES[0], ByteBuffer.wrap("Penelope".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[1], ByteBuffer.wrap("Ortiz".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[2], ByteBuffer.wrap("50100 Bechtelar Turnpike".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[3], ByteBuffer.wrap("02362".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[4], ByteBuffer.wrap("Mississippi".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[5], ByteBuffer.wrap("Turks and Caicos Islands".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[6], ByteBuffer.wrap("(710)173-1052 x897".getBytes(Charsets.UTF_8)))
            .put(COL_NAMES[7], ByteBuffer.allocate(4).putInt(9215))
            .put(COL_NAMES[8], ByteBuffer.allocate(4).putInt(1))
            .build();

    /**
     * Arbitrary {@link UUID} used to identify a row
     * @see GetRowTest
     */
    public static final UUID ROW_UUID = UUID.fromString("e315c6bf-ef4e-4c08-bb95-09a70c98d278");


    /**
     * Arbitrary name of a table used for Honeycomb tests
     */
    public static final String HC_TEST_TABLE = "foo/bar";


    /**
     * Table schema description used for Honeycomb tests
     */
    public static final TableSchema HC_TABLE_SCHEMA = new TableSchema(
            ImmutableList.of(
                    ColumnSchema.builder(COL_NAMES[0], ColumnType.STRING).setMaxLength(32).build(),
                    ColumnSchema.builder(COL_NAMES[1], ColumnType.STRING).setMaxLength(32).build(),
                    ColumnSchema.builder(COL_NAMES[2], ColumnType.STRING).setMaxLength(32).build(),
                    ColumnSchema.builder(COL_NAMES[3], ColumnType.STRING).setMaxLength(16).build(),
                    ColumnSchema.builder(COL_NAMES[4], ColumnType.STRING).setMaxLength(2).build(),
                    ColumnSchema.builder(COL_NAMES[5], ColumnType.STRING).setMaxLength(64).build(),
                    ColumnSchema.builder(COL_NAMES[6], ColumnType.STRING).setMaxLength(32).build(),
                    ColumnSchema.builder(COL_NAMES[7], ColumnType.LONG).build(),
                    ColumnSchema.builder(COL_NAMES[8], ColumnType.LONG).build()
            ),
            ImmutableList.of(
                    new IndexSchema(HC_INDEX_NAME, Lists.newArrayList(COL_NAMES[0]), false)
            )
    );

    /**
     * Query key for an index on one column with a specific value used for Honeycomb tests
     */
    public static QueryKey HC_QUERY_KEY = new QueryKey(HC_INDEX_NAME, QueryType.EXACT_KEY,
            ImmutableMap.<String, ByteBuffer>of(COL_NAMES[0], ByteBuffer.wrap("Penelope".getBytes(Charsets.UTF_8))));


    private DataProvider() {

    }
}
