package io.github.djhworld.sstable;


import java.io.IOException;

interface Block {
    int ENTRY_HEADER_BYTES = 4;

    default int put(String value) throws IOException {
        throw new UnsupportedOperationException();
    }

    default String read(int offsetInBlock) {
        throw new UnsupportedOperationException();
    }
}
