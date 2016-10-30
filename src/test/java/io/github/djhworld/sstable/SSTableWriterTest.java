package io.github.djhworld.sstable;

import io.github.djhworld.io.FileSink;
import io.github.djhworld.io.FileSource;
import io.github.djhworld.model.RowMutation;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Lists.newArrayList;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Paths.get;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SSTableWriterTest {
    private static final Path DATA_DB = get("data.db");
    private static List<RowMutation> MUTATIONS;

    @BeforeClass
    public static void beforeClass() {
        MUTATIONS = newArrayList();
        MUTATIONS.add(newAddMutation("com.amazon", "anchor:four", "testing3"));
        MUTATIONS.add(newAddMutation("com.amazon", "data:test", "testing1"));

        for (int i = 9995; i >= 0; i--) {
            MUTATIONS.add(newAddMutation("com.amazon", "page:" + String.format("%05d", +i) + "column", "value" + i));
        }

        MUTATIONS.add(newAddMutation("com.amazon", "data:rest", "testing2"));
        MUTATIONS.add(newAddMutation("com.amazon", "anchor:five", "testing4"));
    }


    @Before
    public void before() throws Exception {
        deleteIfExists(DATA_DB);
    }

    @Test
    public void shouldWrite() throws Exception {
        FileSink sink = new FileSink(DATA_DB);
        try (SSTableWriter ssTableWriter = new SSTableWriter(sink)) {
            for (RowMutation mutation : MUTATIONS) {
                ssTableWriter.write(mutation.rowKey, mutation.columnKey, mutation.value);
            }
        }
        SSTable ssTable = new SSTable(new FileSource(DATA_DB));
        assertThat(ssTable.noOfRows(), is(MUTATIONS.size()));

        MUTATIONS.sort((rm1, rm2) -> {
            String comparison1 = rm1.rowKey+rm1.columnKey;
            String comparison2 = rm2.rowKey+rm2.columnKey;
            return comparison1.compareTo(comparison2);
        });

        AtomicInteger counter = new AtomicInteger(0);
        ssTable.scan((rm) -> {
            RowMutation mutation = MUTATIONS.get(counter.get());
            assertThat(rm.rowKey, is(mutation.rowKey));
            assertThat(rm.columnKey, is(mutation.columnKey));
            assertThat(rm.value, is(mutation.value));
            counter.incrementAndGet();
        });
    }
}