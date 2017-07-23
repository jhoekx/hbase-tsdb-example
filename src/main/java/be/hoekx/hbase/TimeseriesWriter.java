package be.hoekx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TimeseriesWriter implements Closeable {
    private static final byte[] CF = Bytes.toBytes(TimeseriesDatabase.COLUMN_FAMILY);
    private static final byte[] TS = Bytes.toBytes("ts");
    private static final byte[] VALUE = Bytes.toBytes("value");

    private final Connection connection;

    TimeseriesWriter(Configuration configuration) throws IOException {
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public void writeAll(TagId tag, List<Point> points) throws IOException {
        List<Put> puts = points.stream()
                .map(toPut(tag))
                .collect(Collectors.toList());

        connection.getTable(TableName.valueOf(TimeseriesDatabase.TABLE_NAME))
                .put(puts);
    }

    private Function<Point, Put> toPut(final TagId tag) {
        final byte[] tagKey = Bytes.toBytes(tag.getTagId());
        return point -> {
            long ts = point.getTs().getEpochSecond();
            Put put = new Put(Bytes.add(tagKey, Bytes.toBytes(ts)));
            put.addColumn(CF, TS, Bytes.toBytes(ts));
            put.addColumn(CF, VALUE, Bytes.toBytes(point.getValue()));
            return put;
        };
    }
}
