package io.github.djhworld.io;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compressor {
    CompressedOutputStream newCompressedOutputStream(OutputStream outputStream) throws IOException;

    InputStream newCompressedInputStream(InputStream inputStream) throws IOException;

    CompressionCodec getCodec();
}
