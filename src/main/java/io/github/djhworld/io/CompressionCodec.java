package io.github.djhworld.io;

public enum CompressionCodec {
    GZIP((byte)0x1),
    SNAPPY((byte)0x2);

    private final byte id;

    CompressionCodec(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static CompressionCodec valueOf(byte id) {
        for(CompressionCodec codec : CompressionCodec.values()) {
            if(codec.id == id)
                return codec;
        }
        throw new IllegalArgumentException("Unknown compression codec for ID: " + id);
    }
}
