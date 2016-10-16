package io.github.djhworld.sstable;

import io.github.djhworld.io.FileSink;
import io.github.djhworld.io.FileSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Optional;

import static io.github.djhworld.model.RowMutation.newAddMutation;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Paths.get;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class SSTableWriterTest {

    public static final Path DATA_DB = get("data.db");

    @Before
    public void before() throws Exception {
        deleteIfExists(DATA_DB);
    }

    @Test
    public void shouldWrite() throws Exception {
        FileSink sink = new FileSink(DATA_DB);
        try (SSTableWriter ssTableWriter = new SSTableWriter(sink)) {
            ssTableWriter.write(newAddMutation("com.amazon", "data:test", "testing1"));
            ssTableWriter.write(newAddMutation("com.amazon", "anchor:four", "testing3"));

            for (int i = 9995; i >= 0; i--) {
                ssTableWriter.write(newAddMutation("com.amazon", "page:" + String.format("%05d", +i) + "column", "value" + i));
            }

            ssTableWriter.write(newAddMutation("com.amazon", "data:rest", "testing2"));
            ssTableWriter.write(newAddMutation("com.amazon", "anchor:five", "testing4"));

        }
        SSTable ssTable = new SSTable(new FileSource(DATA_DB));
        assertThat(ssTable.noOfRows(), is(10000));
        assertThat(ssTable.get("com.amazon", "page:00293column"), is(Optional.of("value293")));
    }
}