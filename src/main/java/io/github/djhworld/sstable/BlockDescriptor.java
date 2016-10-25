package io.github.djhworld.sstable;

public class BlockDescriptor {
    public final int offset;
    public final int length;

    public BlockDescriptor(int offset, int length) {
        this.offset = offset;
        this.length = length;
    }
}