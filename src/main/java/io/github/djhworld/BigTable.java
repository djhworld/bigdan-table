package io.github.djhworld;

import com.google.common.collect.Range;
import io.github.djhworld.tablet.TabletMetadataService;
import io.github.djhworld.tablet.TabletServer;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class BigTable {
    public static void main(String[] args) {
        TabletServer tabletServer = new TabletServer(
                new TabletMetadataService(),
                newSingleThreadScheduledExecutor()
        );

        tabletServer.register(Range.open("com", "com1"), "1");
    }

}
