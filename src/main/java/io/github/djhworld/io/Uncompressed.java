package io.github.djhworld.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static io.github.djhworld.io.CompressionType.*;

public class Uncompressed implements CompressionStrategy {

    @Override
    public CompressedOutputStream newOutputStream(OutputStream outputStream) throws IOException {
        return new UncompressedOutputStream(outputStream);
    }

    @Override
    public InputStream newInputStream(InputStream inputStream) throws IOException {
        return inputStream;
    }

    @Override
    public CompressionType getCompressionType() {
        return UNCOMPRESSED;
    }

    private static class UncompressedOutputStream extends CompressedOutputStream {
        private final OutputStream outputStream;

        UncompressedOutputStream(OutputStream os) throws IOException {
            outputStream = os;
        }

        @Override
        public void finish() throws IOException {
            outputStream.flush();
        }

        @Override
        public void write(int b) throws IOException {
            outputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            outputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            outputStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            outputStream.flush();
        }

        @Override
        public void close() throws IOException {
            outputStream.close();
        }
    }
}
