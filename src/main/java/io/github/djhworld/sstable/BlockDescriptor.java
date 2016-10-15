package io.github.djhworld.sstable;

class BlockDescriptor {
    final int blockNumber;
    final int blockOffset;

    private BlockDescriptor(int blockNumber, int blockOffset) {
        this.blockNumber = blockNumber;
        this.blockOffset = blockOffset;
    }

    static BlockDescriptor of(int blockNumber, int blockOffset) {
        return new BlockDescriptor(blockNumber, blockOffset);
    }
}
