package io.github.djhworld.io;

import java.io.IOException;
import java.io.InputStream;

public interface Sink {
    void flush(InputStream inputStream, int length) throws IOException;
}
