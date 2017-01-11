package io.github.djhworld.io;

public class CompressionStrategyFactory {
    public static CompressionStrategy create(CompressionType codec) {
        switch (codec) {
            case GZIP:
                return new Gzip();
            case SNAPPY:
                return new Snappy();
            case UNCOMPRESSED:
                return new Uncompressed();
            default:
                throw new UnsupportedOperationException("Unsupported compression codec " + codec);
        }
    }
}
