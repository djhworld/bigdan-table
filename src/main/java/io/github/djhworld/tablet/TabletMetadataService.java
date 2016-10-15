package io.github.djhworld.tablet;

import io.github.djhworld.log.CommitLog;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.Paths.get;

public class TabletMetadataService {

    public TabletMetadataService() {
    }

    public CommitLog getCurrentCommitLog(String tabletId) {
        return new CommitLog(get("/tmp/log/commit"));

    }

    public int getCurrentTabletGeneration(String tabletId) {
        return 0;

    }

    public void setCurrentTabletGeneration(String tabletId, int newTabletGeneration) {

    }

    public TabletStore getTabletStore(String tabletId) throws IOException {
        //TODO:!!
        Path root = Paths.get("/tmp/sstables");
        return new FileBasedTabletStore(root);
    }
}
