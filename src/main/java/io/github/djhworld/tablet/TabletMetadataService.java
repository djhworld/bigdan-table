package io.github.djhworld.tablet;

import com.amazonaws.services.s3.AmazonS3Client;
import io.github.djhworld.io.CompressionType;
import io.github.djhworld.log.CommitLog;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static io.github.djhworld.io.CompressionType.*;
import static java.nio.file.Paths.get;

//TODO:....
public class TabletMetadataService {
    private Map<String, CommitLog> tabletToCommitLogs;
    private Map<String, Integer> tabletGenerations;

    public TabletMetadataService() throws IOException {
        tabletToCommitLogs = new HashMap<>();
        tabletToCommitLogs.put("0", new CommitLog(get("/tmp/commit0")));
        tabletToCommitLogs.put("1", new CommitLog(get("/tmp/commit1")));

        tabletGenerations = new HashMap<>();
        tabletGenerations.put("0", 1);
        tabletGenerations.put("1", 1);
    }

    public CommitLog getCurrentCommitLog(String tabletId) throws IOException {
        return new CommitLog(get("/tmp/commit" + tabletId));

    }

    public int getCurrentTabletGeneration(String tabletId) {
        return tabletGenerations.get(tabletId);

    }

    public void setCurrentTabletGeneration(String tabletId, int newTabletGeneration) {
        tabletGenerations.put(tabletId, newTabletGeneration);
    }

    public TabletStore getTabletStore(String tabletId) throws IOException {
        //TODO:!!
        //Path root = Paths.get("/tmp/sstables");
        //return new FileBasedTabletStore(root);
        return new S3BasedTabletStore(new AmazonS3Client(), Paths.get("djhworld", "sstables", tabletId));
    }

    public CompressionType getCompressionCodecFor(String tabletId) throws IOException {
        //TODO!!
        return SNAPPY;
    }
}
