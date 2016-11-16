package io.github.djhworld.tablet;

import io.github.djhworld.exception.TabletException;
import io.github.djhworld.log.CommitLog;
import io.github.djhworld.model.RowMutation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.github.djhworld.Matchers.rowMutationMatcher;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static io.github.djhworld.model.RowMutation.newDeleteMutation;
import static java.nio.file.Files.*;
import static java.nio.file.Paths.get;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TabletTest {

    public static final Path TMP_SSTABLES_PATH = get("/Users/danielharper/tmp/sstables/");
    @Mock
    private TabletMetadataService mockedTabletMetadataService;

    @Mock
    private CommitLog mockedCommitLog;
    private FileBasedTabletStore source;


    @Before
    public void before() throws Exception {
        initMocks(this);
        list(TMP_SSTABLES_PATH).forEach((path) -> {
            try {
                if (isDirectory(path)) {
                    list(path).forEach((p) -> {
                        try {
                            deleteIfExists(p);
                        } catch (IOException e) {

                        }
                    });
                }
                deleteIfExists(path);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        source = new FileBasedTabletStore(TMP_SSTABLES_PATH);
        when(mockedTabletMetadataService.getTabletStore(eq("id"))).thenReturn(source);
        when(mockedTabletMetadataService.getCurrentCommitLog(eq("id"))).thenReturn(mockedCommitLog);
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(1);
    }

    @Test
    public void shouldLoadFromCommitLog() throws Exception {
        when(mockedCommitLog.exists()).thenReturn(true);
        when(mockedCommitLog.iterator()).thenReturn(new Iterator<RowMutation>() {
            List<RowMutation> entries = newArrayList(
                    newAddMutation("row1", "col1", "val1"),
                    newAddMutation("row2", "col2", "val2"),
                    newAddMutation("row3", "col3", "val3"),
                    newDeleteMutation("row1", "col1"),
                    newAddMutation("row1", "col1", "valNEW"),
                    newAddMutation("row1", "col1", "valNEW3"),
                    newAddMutation("row1", "col1", "valNEW4"),
                    newDeleteMutation("row3", "col3")
            );
            Iterator<RowMutation> iterator = entries.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public RowMutation next() {
                return iterator.next();
            }
        });

        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        verify(mockedCommitLog, times(1)).exists();
        assertThat(tablet.get("row1", "col1"), is(of("valNEW4")));
        assertThat(tablet.get("row2", "col2"), is(of("val2")));
        assertThat(tablet.get("row3", "col3"), is(empty()));
    }

    @Test
    public void shouldInsertRecords() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c1"));
        tablet.apply(newAddMutation("c", "b", "a1"));
        tablet.apply(newAddMutation("a", "b", "c2"));
        tablet.apply(newAddMutation("c", "b", "a2"));
        assertThat(tablet.size(), is(2));

        assertThat(tablet.get("a", "b"), is(of("c2")));
        assertThat(tablet.get("c", "b"), is(of("a2")));

        InOrder inOrder = inOrder(mockedCommitLog);
        inOrder.verify(mockedCommitLog, times(1)).exists();
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newAddMutation("a", "b", "c1"))));
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newAddMutation("c", "b", "a1"))));
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newAddMutation("a", "b", "c2"))));
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newAddMutation("c", "b", "a2"))));
        verifyNoMoreInteractions(mockedCommitLog);
    }

    @Test
    public void shouldReturnLatestVersionOfRecord() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c1"));
        tablet.apply(newAddMutation("a", "b", "c2"));
        tablet.apply(newAddMutation("a", "b", "c3"));
        assertThat(tablet.size(), is(1));
        assertThat(tablet.get("a", "b"), is(of("c3")));
    }

    @Test
    public void shouldReturnLatestVersionOfRecordAfterFlushing() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c1"));
        tablet.apply(newAddMutation("a", "b", "c2"));
        tablet.apply(newAddMutation("a", "b", "c3"));
        tablet.apply(newAddMutation("a", "b", "c4"));
        tablet.apply(newAddMutation("a", "b", "c5"));
        tablet.apply(newAddMutation("a", "b", "c6"));
        tablet.flush();
        assertThat(tablet.get("a", "b"), is(of("c6")));
    }

    @Test
    public void shouldFlushMax3Versions() throws Exception {
        //TODO: need to get a thing that gets all versions for a key
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c1"));
        tablet.apply(newAddMutation("a", "b", "c2"));
        tablet.apply(newAddMutation("a", "b", "c3"));
        tablet.apply(newAddMutation("a", "b", "c4"));
        tablet.apply(newAddMutation("a", "b", "c5"));
        tablet.apply(newAddMutation("a", "b", "c6"));
        tablet.flush();
        assertThat(tablet.get("a", "b"), is(of("c6")));
    }


    @Test
    public void shouldReturnEmptyForRecordNotFound() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        assertThat(tablet.get("a", "b"), is(empty()));
    }

    @Test
    public void shouldPropagateErrorIfCommitLogFails() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        try {
            doThrow(new IOException("simulated")).when(mockedCommitLog).commit(any(RowMutation.class));
            tablet.apply(newAddMutation("a", "b", "c"));
            fail("Should have caught exception");
        } catch (TabletException e) {
            assertThat(tablet.size(), is(0));
            assertThat(tablet.get("a", "b"), is(empty()));
        }
    }

    @Test
    public void shouldFlushToSSTable() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c"));
        tablet.apply(newAddMutation("c", "b", "a"));

        assertThat(source.list(1).size(), is(0));
        tablet.flush();

        assertThat(tablet.size(), is(0));
        assertThat(source.list(1).size(), is(1));
        assertThat(tablet.get("a", "b"), is(of("c")));
        assertThat(tablet.get("c", "b"), is(of("a")));

        verify(mockedCommitLog, times(1)).checkpoint();
    }


    @Test
    public void shouldReturnEmptyForDeletedRecord() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c"));
        tablet.apply(newDeleteMutation("a", "b"));
        assertThat(tablet.get("a", "b"), is(empty()));

        InOrder inOrder = inOrder(mockedCommitLog);
        inOrder.verify(mockedCommitLog, times(1)).exists();
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newAddMutation("a", "b", "c"))));
        inOrder.verify(mockedCommitLog, times(1)).commit(argThat(rowMutationMatcher(newDeleteMutation("a", "b"))));
        verifyNoMoreInteractions(mockedCommitLog);
    }

    @Test
    public void shouldReturnEmptyForDeletedRecordInSSTable() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("a", "b", "c"));
        tablet.apply(newDeleteMutation("a", "b"));
        tablet.flush();
        assertThat(tablet.get("a", "b"), is(empty()));
    }

    @Test
    public void shouldCompact() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        String foo = "foo";
        String bar1 = "bar1";
        String bar2 = "bar2";

        tablet.apply(newAddMutation(foo, bar1, "v1"));
        tablet.apply(newAddMutation(foo, bar2, "v2"));
        tablet.flush();
        tablet.apply(newAddMutation(foo, bar1, "v3"));
        tablet.apply(newAddMutation(foo, bar2, "v4"));
        tablet.flush();
        tablet.apply(newAddMutation(foo, bar1, "v5"));
        tablet.apply(newAddMutation(foo, bar2, "v6"));
        assertThat(source.list(1).size(), is(2));


        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id")))
                .thenReturn(1)
                .thenReturn(2);

        tablet.compact();

        verify(mockedTabletMetadataService, times(1)).setCurrentTabletGeneration(eq("id"), eq(2));
        assertThat(tablet.currentGeneration(), is(2));
        assertThat(source.list(2).size(), is(1));
        assertThat(tablet.get(foo, bar1), is(of("v5")));
        assertThat(tablet.get(foo, bar2), is(of("v6")));
    }

    @Test
    public void shouldRemoveDeletedItemsOnCompaction() throws Exception {
        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        tablet.apply(newAddMutation("foo", "bar1", "value1"));
        tablet.apply(newAddMutation("foo", "bar2", "value2"));
        tablet.flush();
        tablet.apply(newDeleteMutation("foo", "bar2"));
        tablet.flush();

        //TODO: is this correct? seems a bit strange to rely on the metadata service for current tablet id
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id")))
                .thenReturn(1)
                .thenReturn(2);
        tablet.compact();


        verify(mockedTabletMetadataService, times(1)).setCurrentTabletGeneration(eq("id"), eq(2));
        assertThat(tablet.currentGeneration(), is(2));
        assertThat(source.list(2).size(), is(1));
        assertThat(tablet.get("foo", "bar1"), is(of("value1")));
        assertThat(tablet.get("foo", "bar2"), is(empty()));
    }
}