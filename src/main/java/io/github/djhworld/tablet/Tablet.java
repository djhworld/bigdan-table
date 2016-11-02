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
import static java.time.LocalDateTime.now;
import static java.time.format.DateTimeFormatter.ofPattern;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.slf4j.LoggerFactory.getLogger;

public class Tablet {
    private static final Logger LOGGER = getLogger(Tablet.class);
    private static final DateTimeFormatter FILENAME_FORMATTER = ofPattern("yyyyMMddHHmmssSSS");
    private static final String TOMBSTONE = "Delete![-TOMBSTONE-]Delete!";
    private final String tabletId;
    private final TabletMetadataService metadataService;
    private final AtomicLong approximateMemTableSizeInBytes;
    private final AtomicLong flushCount;
    private final CommitLog commitLog;
    private final TabletStore tabletStore;

    private TreeBasedTable<String, String, String> memTable;
    private Stack<SSTable> ssTables;

    //TODO: metadata service
    //TODO: need to store commit log somewhere more permanent
    //feed it metadata service and it requests the information it needs
    public Tablet(String tabletId, TabletMetadataService tabletMetadataService) {
        try {
            this.tabletId = tabletId;
            this.metadataService = tabletMetadataService;

            this.memTable = create();
            this.flushCount = new AtomicLong(0);
            this.approximateMemTableSizeInBytes = new AtomicLong(0);

            //TODO: when to close?
            this.commitLog = tabletMetadataService.getCurrentCommitLog(tabletId);
            if (commitLog.exists())
                restoreFromCommitLog();

            this.tabletStore = tabletMetadataService.getTabletStore(tabletId);
            this.ssTables = loadSSTables();
        } catch (Exception e) {
            throw new TabletException("Caught error attempting to initialise tablet: ", e);
        }
    }

    public synchronized void apply(RowMutation rowMutation) {
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
        String result = memTable.get(rowKey, columnName);

        if (result != null) {
            if (TOMBSTONE.equals(result))
                return empty();

            return of(result);
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

                TreeBasedTable<String, String, String> oldMemTable = this.memTable;
                Path filename = createSSTable(oldMemTable, currentTabletGeneration);

                this.ssTables.add(
                        new SSTable(
                                tabletStore.get(currentTabletGeneration, filename)
                        )
                );

                this.memTable = create();
                this.flushCount.incrementAndGet();
                this.commitLog.checkpoint();
            } catch (Exception e) {
                throw new TabletException("Caught error attempting to flushTo mem table to SSTable", e);
            }
        }
    }

    //TODO: what if flushing?
    public synchronized void compact() {
        synchronized (ssTables) {
            try {
                if (ssTables.size() > 1) {
                    int currentTabletGeneration = metadataService.getCurrentTabletGeneration(tabletId);

                    LOGGER.info("Compacting tablet generation " + currentTabletGeneration);
                    int newTabletGeneration = currentTabletGeneration + 1;

                    TreeBasedTable<String, String, String> tempTable = create();
                    for (SSTable ssTable : ssTables) {
                        Stopwatch stopwatch = Stopwatch.createStarted();
                        ssTable.scan(rm -> {
                            if (TOMBSTONE.equals(rm.value))
                                tempTable.remove(rm.rowKey, rm.columnKey);
                            else
                                tempTable.put(rm.rowKey, rm.columnKey, rm.value);
                        });
                        LOGGER.info("Took " + stopwatch.stop().elapsed(MILLISECONDS) + "ms to scan");
                    }

                    createSSTable(tempTable, newTabletGeneration);
                    metadataService.setCurrentTabletGeneration(tabletId, newTabletGeneration);
                    ssTables = loadSSTables();
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

        this.memTable.put(rowMutation.rowKey, rowMutation.columnKey, rowMutation.value);
        this.approximateMemTableSizeInBytes.addAndGet(rowMutation.size());
    }

    private void delete(RowMutation rowMutation, boolean requiresCommit) throws IOException {
        if (requiresCommit)
            commitLog.commit(rowMutation);

        this.memTable.put(rowMutation.rowKey, rowMutation.columnKey, TOMBSTONE);
        this.approximateMemTableSizeInBytes.addAndGet((rowMutation.rowKey + rowMutation.columnKey + TOMBSTONE).getBytes().length);
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

    private Stack<SSTable> loadSSTables() throws IOException {
        int currentGeneration = metadataService.getCurrentTabletGeneration(tabletId);

        List<Path> ssTablePaths = tabletStore.list(currentGeneration);
        Stack<SSTable> ssTablesStack = new Stack<>();

        for (Path ssTablePath : ssTablePaths) {
            ssTablesStack.push(new SSTable(tabletStore.get(currentGeneration, ssTablePath)));
        }

        return ssTablesStack;
    }

    private Path createSSTable(TreeBasedTable<String, String, String> data, Integer tabletGeneration) throws IOException {
        Path filename = Paths.get(now().format(FILENAME_FORMATTER) + ".db");
        LOGGER.info("Creating SSTable at path " + filename);

        try (SSTableWriter ssTableWriter = new SSTableWriter(tabletStore.newSink(tabletGeneration, filename))) {
            for (Table.Cell<String, String, String> cell : data.cellSet()) {
                ssTableWriter.write(
                        cell.getRowKey(),
                        cell.getColumnKey(),
                        cell.getValue()
                );
            }
        }

        return filename;
    }
}