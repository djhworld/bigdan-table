package io.github.djhworld.log;

import io.github.djhworld.model.RowMutation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static io.github.djhworld.Matchers.rowMutationMatcher;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static io.github.djhworld.model.RowMutation.newDeleteMutation;
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

    @Test
    public void shouldCheckpointCommitLog() throws Exception {
        List<RowMutation> firstTranch = newArrayList(
                newAddMutation("ro1", "col1", "value"),
                newAddMutation("row2", "col2", "value"),
                newAddMutation("row3", "col3", "value")
        );

        List<RowMutation> secondTranch = newArrayList(
                newAddMutation("row4", "col4", "value"),
                newAddMutation("row5", "col5", "value"),
                newAddMutation("row6", "col6", "value")
        );


        CommitLog commitLog = new CommitLog(TEMP_FILE.toPath());

        for (RowMutation mutation : firstTranch) {
            commitLog.commit(mutation);
        }

        commitLog.checkpoint();

        for (RowMutation mutation : secondTranch) {
            commitLog.commit(mutation);
        }

        int index = 0;
        for (RowMutation committedMutation : commitLog) {
            assertThat(committedMutation, is(rowMutationMatcher(secondTranch.get(index))));
            index++;
        }
    }


    @Test
    public void shouldPropagateErrorIfCheckpointFails() throws Exception {
        Assert.fail("TODO");
    }

}