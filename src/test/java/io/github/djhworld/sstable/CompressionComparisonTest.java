package io.github.djhworld.sstable;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import io.github.djhworld.io.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.base.Stopwatch.createStarted;
import static io.github.djhworld.io.CompressionType.*;
import static java.lang.System.currentTimeMillis;
import static java.nio.file.Files.deleteIfExists;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CompressionComparisonTest {
    private static final int RECORDS = 10_000_000;
    private static final Path GZIP_LOCATION = Paths.get("gzip.db");
    private static final Path SNAPPY_LOCATION = Paths.get("snappy.db");
    private static final Path UNCOMPRESSED_LOCATION = Paths.get("uncompressed.db");

    private Table<String, String, Long> results;

    @Before
    public void setUp() throws Exception {
        cleanup();
        results = TreeBasedTable.create();
    }

    @After
    public void tearDown() throws Exception {
        cleanup();
    }

    private void cleanup() throws IOException {
        deleteIfExists(GZIP_LOCATION);
        deleteIfExists(SNAPPY_LOCATION);
        deleteIfExists(UNCOMPRESSED_LOCATION);
    }

    @Test
    public void testSpeedOfCompressionAlgorithms() throws Exception {
        runTest(GZIP_LOCATION, GZIP);
        runTest(SNAPPY_LOCATION, SNAPPY);
        runTest(UNCOMPRESSED_LOCATION, UNCOMPRESSED);

        System.out.print("\t");
        for (String col : results.columnKeySet()) {
            System.out.print(col + "\t");
        }
        System.out.println();
        for (String row : results.rowKeySet()) {
            System.out.print(row + "\t");
            for (String col : results.columnKeySet()) {
                System.out.print(results.get(row, col) + "\t");
            }
            System.out.println();
        }

    }

    private void runTest(Path path, CompressionType compressionType) throws IOException {
        System.out.println(compressionType + " Test");
        System.out.println(Strings.repeat("-", 100));
        write(new FileSink(path), compressionType);
        SSTable ssTable = open(new FileSource(path), compressionType);
        stream(ssTable);
        results.put(compressionType.toString(), "size", Files.size(path));
        System.out.println("\n");
    }

    private void write(Sink sink, CompressionType compressionType) throws IOException {
        Stopwatch stopwatch = createStarted();
        try (SSTableWriter writer = new SSTableWriter(sink, compressionType)) {
            for (int i = 0; i < RECORDS; i++) {
                writer.write("com.amazon.data.repository", "key", "value" + i, currentTimeMillis());
                if (i % 1000 == 0)
                    writer.write("com.amazon.data.repository", "key" + i, "value" + i, currentTimeMillis());
                if (i % 5000 == 0)
                    writer.write("com.amazon.data.repository" + i, "key", "value" + i, currentTimeMillis());
            }
        }
        results.put(compressionType.toString(), "write", stopwatch.stop().elapsed(MILLISECONDS));
    }

    private SSTable open(Source source, CompressionType compressionType) throws IOException {
        Stopwatch stopwatch = createStarted();
        SSTable ssTable = new SSTable(source);
        results.put(compressionType.toString(), "open", stopwatch.stop().elapsed(MILLISECONDS));
        return ssTable;
    }

    private void stream(SSTable ssTable) throws IOException {
        Stopwatch stopwatch = createStarted();
        ssTable.stream().forEach(cm -> {
        });
        results.put(ssTable.compressionCodec().toString(), "stream", stopwatch.stop().elapsed(MILLISECONDS));
    }
}
