package be.hoekx.hbase;

import java.io.Closeable;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {
}
