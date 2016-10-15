package io.github.djhworld.sstable;

import io.github.djhworld.exception.SSTableException;
import io.github.djhworld.io.FileSource;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.io.Resources.getResource;
import static java.nio.file.Paths.get;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(value = NAME_ASCENDING)
public class SSTableTest {
    public static final String TEST_DATA_DB = "test-data.db";
    public static final int NO_OF_ITEMS = 9999;
    private static SSTable SS_TABLE;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Path path = get(getResource(TEST_DATA_DB).toURI());
        SS_TABLE = new SSTable(new FileSource(path));
    }

    @Test
    public void shouldReturnTrueForItemFound() throws Exception {
        for (int i = 0; i < NO_OF_ITEMS; i++) {
            assertThat(SS_TABLE.contains("com.amazon", "page:" + String.format("%05d", i) + "column"), is(true));
        }

        assertThat(SS_TABLE.contains("com.amazon", "info:text"), is(true));
    }

    @Test
    public void shouldReturnFalseForItemMissing() throws Exception {
        assertThat(SS_TABLE.contains("unknownRow", "page:column0"), is(false));
        assertThat(SS_TABLE.contains("com.amazon", "unknownFamily:column0"), is(false));
        assertThat(SS_TABLE.contains("com.amazon", "page:unknownColumn"), is(false));
    }

    @Test
    public void shouldRetrieveCorrectItemForKey() throws Exception {
        for (int i = 0; i < NO_OF_ITEMS; i++) {
            Optional<String> s = SS_TABLE.get("com.amazon", "page:" + String.format("%05d", i) + "column");
            assertThat(s, is(of("value" + i)));
        }

        assertThat(SS_TABLE.get("com.amazon", "info:text"), is(of("lorem ipsum")));
    }

    @Test
    public void shouldGetCorrectNumberOfBlocks() throws Exception {
        assertThat(SS_TABLE.blocks(), is(3));
    }

    @Test
    public void shouldGetCorrectBlockSize() throws Exception {
        assertThat(SS_TABLE.blockSize(), is(64000));
    }

    @Test(expected = SSTableException.class)
    public void shouldFailToLoadSSTableIfHasInvalidMagic() throws Exception {
        Path path = get(getResource("invalid-magic.db").toURI());
        new SSTable(new FileSource(path));
    }

    @Test(expected = SSTableException.class)
    public void shouldFailToLoadSSTableIfHasCorruptedFooter() throws Exception {
        Path path = get(getResource("invalid-footer.db").toURI());
        new SSTable(new FileSource(path));
    }

    @Test
    public void shouldScanInOrderAndLoadAllBlocksIntoCache() throws Exception {
        Path path = get(getResource(TEST_DATA_DB).toURI());
        SS_TABLE = new SSTable(new FileSource(path));

        assertThat(SS_TABLE.cachedBlocks(), is(0L));

        AtomicInteger i = new AtomicInteger(-1);
        SS_TABLE.scan(rm -> {
            assertThat(SS_TABLE.cachedBlocks(), is(3L));
            if (i.get() == -1) {
                assertThat(rm.rowKey, is("com.amazon"));
                assertThat(rm.columnKey, is("info:text"));
                i.getAndIncrement();
            } else {
                assertThat(rm.rowKey, is("com.amazon"));
                assertThat(rm.columnKey, is("page:" + String.format("%05d", i.getAndIncrement()) + "column"));
            }
        });

    }
}