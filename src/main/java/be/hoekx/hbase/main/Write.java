package be.hoekx.hbase.main;

import be.hoekx.hbase.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Write {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        TimeseriesDatabase db = new TimeseriesDatabase(config);
        try (TimeseriesWriter writer = db.getWriter()) {
            insertData(writer);
        }
    }

    private static void insertData(TimeseriesWriter writer) throws IOException {
        for (int i=0; i<20; i++) {
            Random generator = new Random(i);
            TagId tag = new TagId(i);
            Timer timer = new Timer();
            Instant ts = Instant.parse("2010-01-01T00:00:00Z");
            Instant endDate = Instant.parse("2014-01-01T00:00:00Z");
            timer.tic();
            while (ts.isBefore(endDate)) {
                Instant next = LocalDateTime.ofInstant(ts, ZoneId.systemDefault())
                        .plusMonths(1)
                        .atZone(ZoneId.systemDefault())
                        .toInstant();
                timer.tic();
                List<Point> points = getValues(generator, ts, next);
                System.out.println(timer.toc("Creating points from " + ts + " to " + next));
                timer.tic();
                writer.writeAll(tag, points);
                System.out.println(timer.toc("Writing points (" + i + ") from " + ts + " to " + next));
                ts = next;
            }
            System.out.println(timer.toc("Complete insert"));
        }
    }

    private static List<Point> getValues(final Random generator, final Instant startDate, final Instant endDate) {
        Instant ts = startDate;
        ArrayList<Point> points = new ArrayList<>((int)Duration.between(startDate, endDate).toMinutes());
        while (ts.isBefore(endDate)) {
            points.add(new Point(ts, generator.nextDouble()));
            ts = ts.plus(1, ChronoUnit.MINUTES);
        }
        return points;
    }
}
