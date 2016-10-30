package io.github.djhworld.sstable;

import io.github.djhworld.io.FileSource;
import io.github.djhworld.model.RowMutation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SSTableWriterTest extends AbstractSSTableTest {
    @BeforeClass
    public static void beforeClass() throws IOException {
        init();
    }

    @Test
    public void shouldWriteAndReturnAllValuesInOrder() throws Exception {
        writeSSTable();
        assertThat(TEMP_FILE.exists(), is(true));

        SSTable ssTable = new SSTable(new FileSource(TEMP_FILE.toPath()));
        assertThat(ssTable.noOfRows(), is(MUTATIONS.size()));

        MUTATIONS.sort((rm1, rm2) -> {
            String comparison1 = rm1.rowKey + rm1.columnKey;
            String comparison2 = rm2.rowKey + rm2.columnKey;
            return comparison1.compareTo(comparison2);
        });

        AtomicInteger counter = new AtomicInteger(0);
        ssTable.scan((rm) -> {
            RowMutation mutation = MUTATIONS.get(counter.getAndIncrement());
            assertThat(rm.rowKey, is(mutation.rowKey));
            assertThat(rm.columnKey, is(mutation.columnKey));
            assertThat(rm.value, is(mutation.value));
        });
    }
}