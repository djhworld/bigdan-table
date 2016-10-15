package io.github.djhworld.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class RewindableByteArrayOutputStream extends ByteArrayOutputStream {
    private int written = -1;

    public int rewind() {
        written = count;
        count = 0;
        return written;
    }

    public InputStream toInputStream() {
        if (written == -1) {
            return new ByteArrayInputStream(buf, 0, count);
        }
        return new ByteArrayInputStream(buf, 0, written);
    }
}
