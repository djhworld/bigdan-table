package io.github.djhworld.log;

import io.github.djhworld.model.RowMutation;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static io.github.djhworld.Matchers.rowMutationMatcher;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static io.github.djhworld.model.RowMutation.newDeleteMutation;
import static java.nio.file.Paths.get;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class CommitLogTest {
    public File TEMP_FILE;

    @Before
    public void setUp() throws Exception {
        TEMP_FILE = File.createTempFile("commit" + System.currentTimeMillis(), ".log");
        TEMP_FILE.deleteOnExit();
    }

    @Test
    public void shouldReadFromCommitLog() throws Exception {
        List<RowMutation> expected = newArrayList(
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newAddMutation("row", "key", "column"),
                newDeleteMutation("row", "key")
        );

        CommitLog commitLog = new CommitLog(TEMP_FILE.toPath());

        for (RowMutation mutation : expected) {
            commitLog.commit(mutation);
        }

        int index = 0;
        for (RowMutation committedMutation : commitLog) {
            assertThat(committedMutation, is(rowMutationMatcher(expected.get(index))));
            index++;
        }
    }
}