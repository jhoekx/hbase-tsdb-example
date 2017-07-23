package be.hoekx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;

public class TimeseriesDatabase {

    static final String TABLE_NAME = "ts";
    static final String COLUMN_FAMILY = "t";

    private final Configuration configuration;

    public TimeseriesDatabase(Configuration configuration) throws IOException {
        this.configuration = configuration;
        createTable();
    }

    public TimeseriesWriter getWriter() throws IOException {
        return new TimeseriesWriter(configuration);
    }

    public TimeseriesReader getReader() throws IOException {
        return new TimeseriesReader(configuration);
    }

    private void createTable() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Admin admin = connection.getAdmin()) {
            if (!hasTable(admin)) {
                HTableDescriptor tsTable = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                tsTable.addFamily(new HColumnDescriptor(COLUMN_FAMILY));
                admin.createTable(tsTable);
            }
        }
    }

    private boolean hasTable(Admin admin) throws IOException {
        return Arrays.stream(admin.listTableNames())
                .anyMatch(tableName -> tableName.getNameAsString().equals(TABLE_NAME));
    }
}
