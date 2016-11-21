package io.github.djhworld.tablet;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import io.github.djhworld.exception.TabletException;
import io.github.djhworld.log.CommitLog;
import io.github.djhworld.model.RowMutation;
import io.github.djhworld.sstable.SSTable;
import io.github.djhworld.sstable.SSTableWriter;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.TreeBasedTable.create;
import static io.github.djhworld.model.RowMutation.Action.DEL;
import static io.github.djhworld.model.RowMutation.TOMBSTONE;
import static java.time.LocalDateTime.now;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

public class Tablet {
    private static final Logger LOGGER = getLogger(Tablet.class);
    private static final DateTimeFormatter FILENAME_FORMATTER = ofPattern("yyyyMMddHHmmssSSS");
    private static final int MAX_VERSIONS_TO_KEEP = 3;
    private final String tabletId;
    private final TabletMetadataService metadataService;
    private final AtomicLong approximateMemTableSizeInBytes;
    private final AtomicLong flushCount;
    private final CommitLog commitLog;
    private final TabletStore tabletStore;

    private final TreeBasedTable<String, String, Stack<RowMutation>> memTable;
    private final Stack<SSTable> ssTables;

    //TODO: metadata service
    //TODO: need to store commit log somewhere more permanent
    //feed it metadata service and it requests the information it needs
    public Tablet(String tabletId, TabletMetadataService tabletMetadataService) {
        try {
            this.tabletId = tabletId;
            this.metadataService = tabletMetadataService;

            this.memTable = create();
            this.ssTables = new Stack<>();
            this.flushCount = new AtomicLong(0);
            this.approximateMemTableSizeInBytes = new AtomicLong(0);

            //TODO: when to close?
            this.commitLog = tabletMetadataService.getCurrentCommitLog(tabletId);
            if (commitLog.exists())
                restoreFromCommitLog();

            this.tabletStore = tabletMetadataService.getTabletStore(tabletId);
            this.loadSSTables();
        } catch (Exception e) {
            throw new TabletException("Caught error attempting to initialise tablet: ", e);
        }
    }

    synchronized void apply(RowMutation rowMutation) {
        synchronized (memTable) {
            try {
                switch (rowMutation.action) {
                    case ADD:
                        insert(rowMutation, true);
                        break;
                    case DEL:
                        delete(rowMutation, true);
                        break;
                }
            } catch (Exception e) {
                throw new TabletException("Caught exception attempting to apply mutation ", e);
            }
        }
    }

    public Optional<String> get(String rowKey, String columnName) {
        Stack<RowMutation> rowMutations = memTable.get(rowKey, columnName);

        if (rowMutations != null && !rowMutations.isEmpty()) {
            if (TOMBSTONE.equals(rowMutations.peek().value))
                return empty();

            return of(rowMutations.peek().value);
        }

        for (SSTable ssTable : ssTables) {
            Optional<String> ssTableResult = ssTable.get(rowKey, columnName);

            if (ssTableResult.isPresent()) {
                if (TOMBSTONE.equals(ssTableResult.get()))
                    return empty();

                return ssTableResult;
            }
        }

        return empty();
    }

    //TODO: what if compacting?
    public synchronized void flush() {
        synchronized (memTable) {
            try {
                if (memTable.isEmpty())
                    return;

                LOGGER.info("Flushing mem table to SSTable as it is " + approximateMemTableSizeInBytes.get() + " bytes");
                int currentTabletGeneration = metadataService.getCurrentTabletGeneration(tabletId);
                this.approximateMemTableSizeInBytes.set(0);

                TreeBasedTable<String, String, Stack<RowMutation>> oldMemTable = this.memTable;
                Path filename = createSSTable(oldMemTable, currentTabletGeneration);

                this.ssTables.add(
                        new SSTable(
                                tabletStore.get(currentTabletGeneration, filename)
                        )
                );

                this.memTable.clear();
                this.flushCount.incrementAndGet();
                this.commitLog.checkpoint();
            } catch (Exception e) {
                throw new TabletException("Caught error attempting to flushTo mem table to SSTable", e);
            }
        }
    }

    //TODO: what if flushing?
    synchronized void compact() {
        synchronized (ssTables) {
            try {
                if (ssTables.size() > 1) {
                    int currentTabletGeneration = metadataService.getCurrentTabletGeneration(tabletId);

                    LOGGER.info("Compacting tablet generation " + currentTabletGeneration);
                    int newTabletGeneration = currentTabletGeneration + 1;

                    TreeBasedTable<String, String, Stack<RowMutation>> tempTable = create();
                    for (SSTable ssTable : ssTables) {
                        Stopwatch stopwatch = Stopwatch.createStarted();
                        ssTable.stream().forEach(rm -> {
                            if (TOMBSTONE.equals(rm.value)) {
                                tempTable.remove(rm.rowKey, rm.columnKey);
                            } else {
                                insertIntoTable(tempTable, rm);
                            }
                        });
                        LOGGER.info("Took " + stopwatch.stop().elapsed(MILLISECONDS) + "ms to scan");
                    }

                    createSSTable(tempTable, newTabletGeneration);
                    metadataService.setCurrentTabletGeneration(tabletId, newTabletGeneration);
                    this.loadSSTables();
                }
            } catch (Exception e) {
                throw new TabletException("Caught error attempting to compact tablet", e);
            }
        }
    }

    // TODO: need to implement compact
    public Tablet split() {
        return null;
    }


    public synchronized long approximateMemTableSizeInBytes() {
        synchronized (approximateMemTableSizeInBytes) {
            return approximateMemTableSizeInBytes.get();
        }
    }

    public int size() {
        return memTable.size();
    }

    public String getTabletId() {
        return tabletId;
    }

    public int currentGeneration() {
        return metadataService.getCurrentTabletGeneration(tabletId);
    }

    private void insert(RowMutation rowMutation, boolean requiresCommit) throws IOException {
        //TODO evaluate value length to see if it exceeds block size
        if (requiresCommit)
            commitLog.commit(rowMutation);

        insertIntoTable(memTable, rowMutation);
        this.approximateMemTableSizeInBytes.addAndGet(rowMutation.size());
    }

    private void delete(RowMutation rowMutation, boolean requiresCommit) throws IOException {
        if (requiresCommit)
            commitLog.commit(rowMutation);

        deleteFromTable(memTable, rowMutation);
        this.approximateMemTableSizeInBytes.addAndGet((rowMutation.rowKey + rowMutation.columnKey + TOMBSTONE).getBytes().length);
    }

    private void deleteFromTable(Table<String, String, Stack<RowMutation>> table, RowMutation rm) {
        Stack<RowMutation> rowMutations = table.get(rm.rowKey, rm.columnKey);
        if (rowMutations != null) {
            rowMutations.clear(); //clear all history
        }
        insertIntoTable(table, rm);
    }

    private void insertIntoTable(Table<String, String, Stack<RowMutation>> table, RowMutation rm) {
        Stack<RowMutation> rowMutations = table.get(rm.rowKey, rm.columnKey);
        if (rowMutations == null) {
            rowMutations = new Stack<>();
            table.put(rm.rowKey, rm.columnKey, rowMutations);
        }

        //if top is a delete action ,then remove it as we are reading
        if (!rowMutations.isEmpty() && DEL.equals(rowMutations.peek())) {
            rowMutations.pop();
        }

        rowMutations.push(rm);
    }

    private void restoreFromCommitLog() throws IOException {
        LOGGER.info("Restoring from commit log....");
        for (RowMutation rowMutation : commitLog) {
            switch (rowMutation.action) {
                case ADD:
                    insert(rowMutation, false);
                    break;
                case DEL:
                    delete(rowMutation, false);
                    break;
            }
        }
    }

    private void loadSSTables() throws IOException {
        int currentGeneration = metadataService.getCurrentTabletGeneration(tabletId);

        List<Path> ssTablePaths = tabletStore.list(currentGeneration);
        Stack<SSTable> ssTablesStack = new Stack<>();

        for (Path ssTablePath : ssTablePaths) {
            ssTablesStack.push(new SSTable(tabletStore.get(currentGeneration, ssTablePath)));
        }

        ssTables.clear();
        ssTables.addAll(ssTablesStack);
    }

    private Path createSSTable(TreeBasedTable<String, String, Stack<RowMutation>> data, Integer tabletGeneration) throws IOException {
        Path filename = Paths.get(now().format(FILENAME_FORMATTER) + ".db");
        LOGGER.info("Creating SSTable at path " + filename);


        try (SSTableWriter ssTableWriter = new SSTableWriter(tabletStore.newSink(tabletGeneration, filename))) {
            for (Table.Cell<String, String, Stack<RowMutation>> cell : data.cellSet()) {
                Stack<RowMutation> mutationVersions = cell.getValue();
                int versionsCount = 0;
                for (int i = mutationVersions.size() - 1; i >= 0; i--) {
                    if(versionsCount == MAX_VERSIONS_TO_KEEP) break;

                    RowMutation mutationVersion = mutationVersions.get(i);
                    ssTableWriter.write(
                            mutationVersion.rowKey,
                            mutationVersion.columnKey,
                            mutationVersion.value,
                            mutationVersion.timestamp
                    );
                    versionsCount++;
                }
            }
        }

        return filename;
    }
}