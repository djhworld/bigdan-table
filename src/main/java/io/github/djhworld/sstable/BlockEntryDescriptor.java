package io.github.djhworld.sstable;

class BlockEntryDescriptor {
    final int id;
    final int offset;

    private BlockEntryDescriptor(int id, int offset) {
        this.id = id;
        this.offset = offset;
    }

    static BlockEntryDescriptor of(int id, int offset) {
        return new BlockEntryDescriptor(id, offset);
    }
}
