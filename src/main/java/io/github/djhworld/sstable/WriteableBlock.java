package io.github.djhworld.sstable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.allocate;

/**
 * Values are packed in the block
 * in the format, where length is the
 * length of the header+value
 * <p>
 * [length][     value     ][length][     value     ]...
 * <--4------------n------->
 */
class WriteableBlock implements Block {
    private final ByteBuffer buffer;
    private final int blockSize;

    WriteableBlock(int blockSize) {
        this.buffer = allocate(blockSize);
        this.blockSize = blockSize;
    }

    public synchronized int put(String value) throws IOException {
        byte[] bytes = value.getBytes();
        int blockOffset = buffer.position();
        buffer.putInt(bytes.length + ENTRY_HEADER_BYTES);
        buffer.put(bytes);
        return blockOffset;
    }

    public synchronized void flushTo(OutputStream out) throws IOException {
        out.write(this.buffer.array());
    }

    boolean hasEnoughSpaceFor(String value) {
        return (remainingSpace() - (value.getBytes().length + ENTRY_HEADER_BYTES)) > 0;
    }

    private int remainingSpace() {
        return blockSize - buffer.position();
    }
}
