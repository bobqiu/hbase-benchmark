package util;

import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.nearinfinity.honeycomb.hbase.HBaseModule;
import com.nearinfinity.honeycomb.hbase.HBaseStore;
import com.nearinfinity.honeycomb.hbase.config.ConfigConstants;
import com.nearinfinity.honeycomb.hbase.rowkey.IndexRowKeyBuilder;
import com.nearinfinity.honeycomb.hbase.rowkey.SortOrder;
import com.nearinfinity.honeycomb.mysql.QueryKey;
import com.nearinfinity.honeycomb.mysql.gen.QueryType;
import com.nearinfinity.honeycomb.mysql.schema.TableSchema;
import config.Config;
import org.apache.hadoop.hbase.HConstants;

import java.nio.ByteBuffer;
import java.util.Map;

public class HoneycombQueryGenerator {
    public static byte[][] generate(Config appConfig) {
        Map<String, String> map = Maps.newHashMap();
        map.put(HConstants.ZOOKEEPER_QUORUM, appConfig.getZkQuorum());
        map.put(ConfigConstants.TABLE_NAME, appConfig.getToolTable());
        map.put(ConfigConstants.COLUMN_FAMILY, appConfig.getColumnFamily());
        Injector injector = Guice.createInjector(new HBaseModule(map));
        HBaseStore store = injector.getInstance(HBaseStore.class);
        long tableId = store.getTableId(appConfig.getSqlTable());
        long indexId = store.getIndexId(tableId, appConfig.getIndexName());
        TableSchema schema = store.getSchema(tableId);
        IndexRowKeyBuilder builder = IndexRowKeyBuilder
                .newBuilder(tableId, indexId)
                .withSortOrder(SortOrder.Ascending);
        final int length = 5000;
        byte[][] generated = new byte[length][];
        for (int i = 0; i < length; i++) {
            generated[i] = generatePersonQuery(schema, builder);
        }

        return generated;
    }

    private static byte[] generatePersonQuery(TableSchema schema, IndexRowKeyBuilder builder) {
        Map<String, ByteBuffer> values = Maps.newHashMap();
        ByteBuffer value = (ByteBuffer) ByteBuffer.allocate(8).putLong(Utils.RANDOM.nextLong() % 100000).rewind();
        values.put("salary", value);
        QueryKey key = new QueryKey("salary", QueryType.KEY_OR_NEXT, values);
        return builder.withQueryKey(key,schema).build().encode();
    }
}