package io.github.djhworld.tablet;

import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import io.github.djhworld.model.RowMutation;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.TreeRangeMap.create;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TabletServer {
    public static final int TABLET_MEMTABLE_LIMIT = 10_000_000;
    private final TabletMetadataService metadataService;
    private final RangeMap<String, String> rowRangeToTabletsMap;
    private final Map<String, Tablet> tabletIdToTabletMap;
    private final ScheduledExecutorService executorService;

    public TabletServer(TabletMetadataService metadataService, ScheduledExecutorService executorService) {
        this.metadataService = metadataService;
        this.rowRangeToTabletsMap = create();
        this.tabletIdToTabletMap = newHashMap();
        this.executorService = executorService;
        startScheduledCompaction();
    }

    public synchronized void register(Range<String> rowRange, String tabletId) {
        rowRangeToTabletsMap.put(rowRange, tabletId);
        tabletIdToTabletMap.put(tabletId, new Tablet(tabletId, metadataService));
        //TODO: this needs committing somewhere
    }

    public synchronized void remove(Range<String> rowRange) {
        //TODO: what if compaction is happening!
        String tabletId = rowRangeToTabletsMap.get(rowRange.upperEndpoint());
        tabletIdToTabletMap.remove(tabletId);
        rowRangeToTabletsMap.remove(rowRange);
        //TODO: this needs committing somewhere
    }

    public void apply(RowMutation rowMutation) {
        Tablet tabletFor = getTabletFor(rowMutation.rowKey);

        //TODO: this should be placed on tablet instead?
        if (tabletFor.approximateMemTableSizeInBytes() > TABLET_MEMTABLE_LIMIT) {
            tabletFor.flush();
        }

        tabletFor.apply(rowMutation);
    }

    public Optional<String> get(String rowKey, String columnKey) {
        return getTabletFor(rowKey).get(rowKey, columnKey);
    }

    private Tablet getTabletFor(String rowKey) {
        String tabletId = rowRangeToTabletsMap.get(rowKey);

        if (tabletId == null)
            throw new IllegalStateException("Tablet server cannot serve this request!");

        return tabletIdToTabletMap.get(tabletId);
    }

    private void startScheduledCompaction() {
        executorService.scheduleAtFixedRate(() -> {
            this.tabletIdToTabletMap.forEach((tabletId, tablet) -> {
                try {
                    tablet.compact();
                } catch (Exception e) {
                    // TODO: what happens if the tablet cannot compact?
                }
            });
        }, 0, 10, MINUTES);
    }
}
