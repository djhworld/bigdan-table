package io.github.djhworld.io;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Sink {
    void flush(ByteBuffer buffer) throws IOException;
}
