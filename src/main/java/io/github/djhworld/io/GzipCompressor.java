package io.github.djhworld.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static io.github.djhworld.io.CompressionCodec.*;

public class GzipCompressor implements Compressor {
    @Override
    public CompressedOutputStream newCompressedOutputStream(OutputStream outputStream) throws IOException {
        return new GzipCompressedOutputStream(outputStream);
    }

    @Override
    public InputStream newCompressedInputStream(InputStream inputStream) throws IOException {
        return new GZIPInputStream(inputStream);
    }

    @Override
    public CompressionCodec getCodec() {
        return GZIP;
    }

    private static class GzipCompressedOutputStream extends CompressedOutputStream {
        private final GZIPOutputStream gzipOutputStream;

        GzipCompressedOutputStream(OutputStream outputStream) throws IOException {
            gzipOutputStream = new GZIPOutputStream(outputStream);
        }

        @Override
        public void finish() throws IOException {
            gzipOutputStream.finish();
        }

        @Override
        public void write(int b) throws IOException {
            gzipOutputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            gzipOutputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            gzipOutputStream.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            gzipOutputStream.flush();
        }

        @Override
        public void close() throws IOException {
            gzipOutputStream.close();
        }
    }
}
