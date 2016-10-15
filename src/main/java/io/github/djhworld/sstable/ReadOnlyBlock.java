package io.github.djhworld.sstable;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newTreeMap;
import static java.nio.ByteBuffer.wrap;

class ReadOnlyBlock implements Block {
    private final Map<Integer, String> entries;
    private int maxOffset;

    ReadOnlyBlock(byte[] from) {
        this.entries = newTreeMap();
        this.maxOffset = 0;
        loadEntries(from);
    }

    public String read(int offsetInBlock) {
        checkArgument(offsetInBlock <= maxOffset, "Invalid offset requested!");
        return entries.get(offsetInBlock);
    }

    public int getMaxOffset() {
        return maxOffset;
    }

    private void loadEntries(byte[] block) {
        ByteBuffer buffer = wrap(block);
        int position = buffer.position();

        while (position < block.length) {
            if (buffer.remaining() < 4)
                break;

            int length = buffer.getInt();

            if (length == 0)
                break;

            byte[] bytes = new byte[length - ENTRY_HEADER_BYTES];
            buffer.get(bytes);
            entries.put(position, new String(bytes));
            maxOffset = position;
            position = buffer.position();
        }

    }
}
