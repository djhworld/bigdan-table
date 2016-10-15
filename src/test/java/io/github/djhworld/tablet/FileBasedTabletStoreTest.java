package io.github.djhworld.tablet;

import org.junit.Test;

import java.nio.file.Paths;

public class FileBasedTabletStoreTest {

    @Test
    public void shouldName() throws Exception {
        FileBasedTabletStore fileBasedTabletStore = new FileBasedTabletStore(Paths.get("/tmp/"));
        System.out.println(fileBasedTabletStore.list(1));
    }
}