package io.github.djhworld.sstable;

import io.github.djhworld.io.CompressionCodec;
import io.github.djhworld.io.FileSink;
import io.github.djhworld.io.GzipCompressor;
import io.github.djhworld.model.RowMutation;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.github.djhworld.io.CompressionCodec.*;
import static io.github.djhworld.model.RowMutation.newAddMutation;

public class AbstractSSTableTest {
    static List<RowMutation> MUTATIONS;
    static File TEMP_FILE;
    static File EMPTY_FILE;

    static void init() throws IOException {
        TEMP_FILE = File.createTempFile("" + System.currentTimeMillis(), ".db");
        TEMP_FILE.deleteOnExit();
        EMPTY_FILE = File.createTempFile("empty" + System.currentTimeMillis(), ".db");
        EMPTY_FILE.deleteOnExit();

        MUTATIONS = newArrayList();
        MUTATIONS.add(newAddMutation("com.amazon", "anchor:four", "testing3"));
        MUTATIONS.add(newAddMutation("com.amazon", "data:test", "testing1"));

        for (int i = 9995; i >= 0; i--) {
            MUTATIONS.add(newAddMutation("com.amazon", "page:" + String.format("%05d", +i) + "column", "value" + i));
        }

        MUTATIONS.add(newAddMutation("com.amazon", "data:rest", "testing2"));
        MUTATIONS.add(newAddMutation("com.amazon", "anchor:five", "testing4"));
    }

    static void writeSSTable() throws IOException {
        FileSink sink = new FileSink(TEMP_FILE.toPath());
        try (SSTableWriter ssTableWriter = new SSTableWriter(sink, GZIP)) {
            for (RowMutation mutation : MUTATIONS) {
                ssTableWriter.write(mutation.rowKey, mutation.columnKey, mutation.value, mutation.timestamp);
            }
        }
    }

    static void writeEmptySSTable() throws IOException {
        FileSink sink = new FileSink(EMPTY_FILE.toPath());
        try (SSTableWriter ssTableWriter = new SSTableWriter(sink, GZIP)) {
        }
    }
}
