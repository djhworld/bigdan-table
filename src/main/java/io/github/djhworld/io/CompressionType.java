package io.github.djhworld.io;

public enum CompressionType {
    UNCOMPRESSED((byte)0x0),
    GZIP((byte)0x1),
    SNAPPY((byte)0x2);

    private final byte id;

    CompressionType(byte id) {
        this.id = id;
    }

    public byte getId() {
        return id;
    }

    public static CompressionType valueOf(byte id) {
        for(CompressionType codec : CompressionType.values()) {
            if(codec.id == id)
                return codec;
        }
        throw new IllegalArgumentException("Unknown compression codec for ID: " + id);
    }
}
