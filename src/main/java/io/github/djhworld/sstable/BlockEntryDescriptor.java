package io.github.djhworld.sstable;

class BlockEntryDescriptor {
    final int id;
    final long timestamp;
    final int offset;

    private BlockEntryDescriptor(int id, long timestamp, int offset) {
        this.id = id;
        this.timestamp = timestamp;
        this.offset = offset;
    }

    static BlockEntryDescriptor of(int id, long timestamp, int offset) {
        return new BlockEntryDescriptor(id, timestamp, offset);
    }
}
