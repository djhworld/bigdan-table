package io.github.djhworld.io;

public class CompressorFactory {
    public static Compressor create(CompressionCodec codec) {
        switch (codec) {
            case GZIP:
                return new GzipCompressor();
            case SNAPPY:
                return new SnappyCompressor();
            default:
                throw new UnsupportedOperationException("Unsupported compression codec " + codec);
        }
    }
}
