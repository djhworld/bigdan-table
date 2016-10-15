package io.github.djhworld.tablet;

import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.TreeBasedTable;
import io.github.djhworld.log.CommitLog;
import io.github.djhworld.model.RowMutation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import static io.github.djhworld.model.RowMutation.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class S3BasedTabletStoreTest {
    @Mock
    private TabletMetadataService mockedTabletMetadataService;

    @Mock
    private CommitLog mockedCommitLog;

    private TabletStore tabletStore;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        AmazonS3Client amazonS3 = new AmazonS3Client();
        tabletStore = new S3BasedTabletStore(amazonS3, Paths.get("djhworld", "sstables"));
      //   tabletStore = new FileBasedTabletStore(Paths.get("/Users/danielharper/tmp/data"));
    }

    @Test
    public void shouldName2() throws Exception {
        when(mockedTabletMetadataService.getTabletStore(eq("id"))).thenReturn(tabletStore);
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(1);
        when(mockedTabletMetadataService.getCurrentCommitLog(eq("id"))).thenReturn(mockedCommitLog);

        //TODO: looks like s3 block limit is 8,192

        Tablet tablet = new Tablet("id", mockedTabletMetadataService);
        for (int i = 0; i < 33333; i++) {
            for (int j = 0; j < 10; j++) {
                tablet.apply(newAddMutation("com.amazon" + i, "site" + j, "data data data data dara data data data" + i));
                tablet.apply(newAddMutation("com.amazon" + i, "page" + j, "<html><head><body>"));
                tablet.apply(newAddMutation("com.amazon" + i, "data" + j, "yep"));
            }
        }
        tablet.flush();
        for (int i = 0; i < 33333; i++) {
            for (int j = 0; j < 10; j++) {
                tablet.apply(newAddMutation("com.amazon" + i, "site" + j, "data data data data dara data data data" + i));
                tablet.apply(newAddMutation("com.amazon" + i, "page" + j, "<html><head><body>"));
                tablet.apply(newAddMutation("com.amazon" + i, "data" + j, "yep"));
            }
        }
        tablet.flush();
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(1).thenReturn(2);
        tablet.compact();

        for (int i = 0; i < 10_00; i++) {
            tablet.apply(newAddMutation("com.amazon", "1site" + i, "data"));
            tablet.apply(newAddMutation("com.amazon", "1page" + i, "<html>"));
            tablet.apply(newAddMutation("com.amazon", "1data" + i, "yep"));
        }

        //TODO: think about range based Sources rather than reading whole input stream
        System.out.println(tablet.get("com.amazon300", "site9"));

    }
}