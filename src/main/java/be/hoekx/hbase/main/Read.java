package be.hoekx.hbase.main;

import be.hoekx.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

public class Read {
    public static void main(String[] args) throws IOException{
        Configuration config = HBaseConfiguration.create();
        TimeseriesDatabase db = new TimeseriesDatabase(config);

        Timer timer = new Timer();
        timer.tic();
        int count = 0;
        Point max = null;
        try (TimeseriesReader reader = db.getReader()) {
            try (CloseableIterable<Point> points = reader.read(new TagId(4))) {
                for (Point point : points) {
                    count++;
                    if (max == null) {
                        max = point;
                    }
                    if (max.getValue() < point.getValue()) {
                        max = point;
                    }
                }
            }
        }
        System.out.println(max);
        System.out.println(timer.toc("Read done (" + count + " points)"));
    }
}
