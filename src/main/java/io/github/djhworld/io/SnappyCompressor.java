package io.github.djhworld.io;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static io.github.djhworld.io.CompressionCodec.*;

public class SnappyCompressor implements Compressor {
    @Override
    public InputStream newCompressedInputStream(InputStream inputStream) throws IOException {
        return new SnappyInputStream(inputStream);
    }

    @Override
    public CompressedOutputStream newCompressedOutputStream(OutputStream outputStream) throws IOException {
        return new SnappyCompressedOutputStream(outputStream);
    }

    @Override
    public CompressionCodec getCodec() {
        return SNAPPY;
    }

    private static class SnappyCompressedOutputStream extends CompressedOutputStream {
        private final SnappyOutputStream snappyOutputStream;

        SnappyCompressedOutputStream(OutputStream outputStream) throws IOException {
            snappyOutputStream = new SnappyOutputStream(outputStream);
        }

        @Override
        public void finish() throws IOException {
            snappyOutputStream.flush();
        }

        @Override
        public void write(int b) throws IOException {
            snappyOutputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            snappyOutputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            snappyOutputStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            snappyOutputStream.flush();
        }

        @Override
        public void close() throws IOException {
            snappyOutputStream.close();
        }
    }
}
