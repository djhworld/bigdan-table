package io.github.djhworld.sstable;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newTreeMap;
import static java.nio.ByteBuffer.wrap;

class ReadOnlyBlock implements Block {
    private final Map<Integer, String> entries;

    ReadOnlyBlock(byte[] from) {
        this.entries = newTreeMap();
        loadEntries(from);
    }

    public String read(int offsetInBlock) {
        checkArgument(entries.containsKey(offsetInBlock), "Invalid offset requested! (" + offsetInBlock + ")");
        return entries.get(offsetInBlock);
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
            position = buffer.position();
        }
    }
}