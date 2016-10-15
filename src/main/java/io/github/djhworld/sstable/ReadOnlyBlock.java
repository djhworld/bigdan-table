package io.github.djhworld.sstable;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;

class ReadOnlyBlock implements Block {
    private final byte[] block;

    ReadOnlyBlock(byte[] from) {
        this.block = from;
    }

    public String read(int offsetInBlock) {
        ByteBuffer buffer = wrap(this.block);
        buffer.position(offsetInBlock);
        int length = buffer.getInt();
        byte[] bytes = new byte[length - ENTRY_HEADER_BYTES];
        buffer.get(bytes);
        return new String(bytes);
    }
}
