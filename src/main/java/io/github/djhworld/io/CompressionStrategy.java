package io.github.djhworld.io;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CompressionStrategy {
    CompressedOutputStream newOutputStream(OutputStream outputStream) throws IOException;

    InputStream newInputStream(InputStream inputStream) throws IOException;

    CompressionType getCompressionType();
}
