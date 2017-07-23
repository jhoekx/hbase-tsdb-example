package be.hoekx.hbase;

import java.time.Instant;

public class Point {
    private final Instant ts;
    private final double value;

    public Point(Instant ts, double value) {
        this.ts = ts;
        this.value = value;
    }

    public Instant getTs() {
        return ts;
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Point{" +
                "ts=" + ts +
                ", value=" + value +
                '}';
    }
}
