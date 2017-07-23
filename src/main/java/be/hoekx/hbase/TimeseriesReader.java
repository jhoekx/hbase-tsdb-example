package be.hoekx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeseriesReader implements Closeable {
    private static final byte[] CF = Bytes.toBytes(TimeseriesDatabase.COLUMN_FAMILY);
    private static final byte[] TS = Bytes.toBytes("ts");
    private static final byte[] VALUE = Bytes.toBytes("value");

    private final Connection connection;

    public TimeseriesReader(Configuration configuration) throws  IOException {
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    @Override
    public void close() throws IOException {
        connection.close();
    }

    public CloseableIterable<Point> read(TagId tag) throws IOException{
        return new TimeseriesIterable(tag);
    }

    private class TimeseriesIterable implements CloseableIterable<Point> {
        private final Table tsdb;
        private final TagId tag;

        private final List<ResultScanner> scanners = new ArrayList<>();

        TimeseriesIterable(TagId tag) throws IOException {
            tsdb = connection.getTable(TableName.valueOf(TimeseriesDatabase.TABLE_NAME));
            this.tag = tag;
        }

        @Override
        public void close() {
            for (ResultScanner scanner : scanners) {
                scanner.close();
            }
        }

        @Override
        public Iterator<Point> iterator() {
            try {
                Scan scan = new Scan();

                /*
                 * Using setRowPrefixFilter combines setting a start row and a filter
                 */
                scan.setRowPrefixFilter(Bytes.toBytes(tag.getTagId()));

                scan.addColumn(CF, TS);
                scan.addColumn(CF, VALUE);

                ResultScanner scanner = tsdb.getScanner(scan);
                scanners.add(scanner);

                return new TimeseriesIterator(scanner.iterator());
            } catch (IOException err) {
                throw new RuntimeException(err);
            }
        }
    }

    private class TimeseriesIterator implements Iterator<Point> {
        private final Iterator<Result> results;

        TimeseriesIterator(Iterator<Result> results) {
            this.results = results;
        }

        @Override
        public boolean hasNext() {
            return results.hasNext();
        }

        @Override
        public Point next() {
            Result r = results.next();
            long ts = Bytes.toLong(r.getValue(CF, TS));
            double value = Bytes.toDouble(r.getValue(CF, VALUE));
            return new Point(Instant.ofEpochSecond(ts), value);
        }
    }
}
