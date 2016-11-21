package io.github.djhworld.sstable;

import io.github.djhworld.exception.SSTableException;
import io.github.djhworld.io.FileSource;
import io.github.djhworld.model.RowMutation;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static java.nio.file.Paths.get;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

@FixMethodOrder(value = NAME_ASCENDING)
public class SSTableTest extends AbstractSSTableTest {
    static final int NO_OF_ITEMS = 9996;
    private static SSTable SS_TABLE;
    private static SSTable EMPTY_SS_TABLE;

    @BeforeClass
    public static void beforeClass() throws IOException {
        init();
        writeSSTable();
        writeEmptySSTable();
        SS_TABLE = new SSTable(new FileSource(TEMP_FILE.toPath()));
        EMPTY_SS_TABLE = new SSTable(new FileSource(EMPTY_FILE.toPath()));
    }

    @Test
    public void shouldReturnTrueForItemFound() throws Exception {
        for (int i = 0; i < NO_OF_ITEMS; i++) {
            assertThat(SS_TABLE.contains("com.amazon", "page:" + String.format("%05d", i) + "column"), is(true));
        }

        assertThat(SS_TABLE.contains("com.amazon", "data:test"), is(true));
        assertThat(SS_TABLE.contains("com.amazon", "data:rest"), is(true));
        assertThat(SS_TABLE.contains("com.amazon", "anchor:four"), is(true));
        assertThat(SS_TABLE.contains("com.amazon", "anchor:five"), is(true));
    }

    @Test
    public void shouldReturnFalseForItemMissing() throws Exception {
        assertThat(SS_TABLE.contains("unknownRow", "page:column0"), is(false));
        assertThat(SS_TABLE.contains("com.amazon", "unknownFamily:column0"), is(false));
        assertThat(SS_TABLE.contains("com.amazon", "page:unknownColumn"), is(false));
    }

    @Test
    public void shouldReturnEmptyForItemMissing() throws Exception {
        assertThat(SS_TABLE.get("unknownRow", "page:column0"), is(empty()));
        assertThat(SS_TABLE.get("com.amazon", "unknownFamily:column0"), is(empty()));
        assertThat(SS_TABLE.get("com.amazon", "page:unknownColumn"), is(empty()));
    }


    @Test
    public void shouldRetrieveCorrectItemForKey() throws Exception {
        for (int i = 0; i < NO_OF_ITEMS; i++) {
            Optional<String> s = SS_TABLE.get("com.amazon", "page:" + String.format("%05d", i) + "column");
            assertThat(s, is(of("value" + i)));
        }

        assertThat(SS_TABLE.get("com.amazon", "data:test"), is(of("testing1")));
        assertThat(SS_TABLE.get("com.amazon", "data:rest"), is(of("testing2")));
        assertThat(SS_TABLE.get("com.amazon", "anchor:four"), is(of("testing3")));
        assertThat(SS_TABLE.get("com.amazon", "anchor:five"), is(of("testing4")));
    }

    @Test
    public void shouldGetCorrectNumberOfBlocks() throws Exception {
        assertThat(SS_TABLE.blocks(), is(3));
        assertThat(EMPTY_SS_TABLE.blocks(), is(1));
    }

    @Test
    public void shouldGetCorrectBlockSize() throws Exception {
        assertThat(SS_TABLE.blockSize(), is(64000));
    }

    //TODO: these tests are wrong
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
    public void shouldScanAllColumnsForRow() throws Exception {
        Stream<RowMutation> stream = SS_TABLE.stream("com.amazon");
        assertThat(stream.count(), is(NO_OF_ITEMS + 4L));
    }

    @Test
    public void shouldReturnEmptyStreamWhenScanningRowThatDoesNotExist() throws Exception {
        Stream<RowMutation> stream = SS_TABLE.stream("foo");
        assertThat(stream.count(), is(0L));
    }

    @Test
    public void shouldReturnEmptyStreamWhenScanningRowOnEmptySSTable() throws Exception {
        Stream<RowMutation> stream = EMPTY_SS_TABLE.stream("com.amazon");
        assertThat(stream.count(), is(0L));
    }

    @Test
    public void shouldOnlyScanRowsForColumnFamily() throws Exception {
        Stream<RowMutation> stream = SS_TABLE.stream("com.amazon", "page");
        List<RowMutation> actual = stream.collect(Collectors.toList());

        assertThat(actual.size(), is(NO_OF_ITEMS));
        for (int i = 0; i < NO_OF_ITEMS; i++) {
            assertThat(actual.get(i).rowKey, is("com.amazon"));
            assertThat(actual.get(i).columnKey, is("page:" + String.format("%05d", i) + "column"));
            assertThat(actual.get(i).value, is("value" + i));
        }
    }

    @Test
    public void shouldReturnEmptyStreamWhenScanningRowForColumnFamilyThatDoesNotExist() throws Exception {
        Stream<RowMutation> stream = SS_TABLE.stream("com.amazon", "da");
        assertThat(stream.count(), is(0L));
    }

    @Test
    public void shouldReturnEmptyStreamWhenScanningEmptyTable() throws Exception {
        assertThat(EMPTY_SS_TABLE.stream().count(), is(0L));
    }

    @Test
    public void shouldScanInOrderAndLoadAllBlocksIntoCache() throws Exception {
        SS_TABLE = new SSTable(new FileSource(TEMP_FILE.toPath()));

        assertThat(SS_TABLE.cachedBlocks(), is(0L));

        AtomicInteger i = new AtomicInteger(-4);
        SS_TABLE.stream().forEach(rm -> {
            assertThat(SS_TABLE.cachedBlocks(), is(3L));
            switch (i.get()) {
                case -4:
                    assertThat(rm.rowKey, is("com.amazon"));
                    assertThat(rm.columnKey, is("anchor:five"));
                    i.getAndIncrement();
                    break;
                case -3:
                    assertThat(rm.rowKey, is("com.amazon"));
                    assertThat(rm.columnKey, is("anchor:four"));
                    i.getAndIncrement();
                    break;
                case -2:
                    assertThat(rm.rowKey, is("com.amazon"));
                    assertThat(rm.columnKey, is("data:rest"));
                    i.getAndIncrement();
                    break;
                case -1:
                    assertThat(rm.rowKey, is("com.amazon"));
                    assertThat(rm.columnKey, is("data:test"));
                    i.getAndIncrement();
                    break;
                default:
                    assertThat(rm.rowKey, is("com.amazon"));
                    assertThat(rm.columnKey, is("page:" + String.format("%05d", i.getAndIncrement()) + "column"));
            }
        });

    }
}