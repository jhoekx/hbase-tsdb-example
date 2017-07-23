package be.hoekx.hbase;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class Timer {

    private final Deque<Instant> tics = new ArrayDeque<>();

    public void tic() {
        tics.push(Instant.now());
    }

    public String toc(String what) {
        return what + " took " + Duration.between(tics.pop(), Instant.now()).toMillis() + "ms";
    }
}
