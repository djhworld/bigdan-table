package io.github.djhworld.tablet;

import com.amazonaws.services.s3.AmazonS3Client;
import io.github.djhworld.log.CommitLog;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.file.Paths;
import java.util.Optional;

import static io.github.djhworld.model.RowMutation.newAddMutation;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
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
    }

    @Test
    public void shouldName2() throws Exception {
        when(mockedTabletMetadataService.getTabletStore(eq("id"))).thenReturn(tabletStore);
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(1);
        when(mockedTabletMetadataService.getCurrentCommitLog(eq("id"))).thenReturn(mockedCommitLog);


        Tablet tablet = new Tablet("id", mockedTabletMetadataService);

        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 500000; i++) {
                tablet.apply(newAddMutation("com.amazon", String.format("%05d", i) + "site", "data data data data dara data data data" + i));
                //tablet.apply(newAddMutation("com.google", String.format("%05d", i) +  "page", "data data data data dara data data data" + i));
            }
            tablet.flush();
        }
        tablet.compact();
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(2);
        for (int j = 0; j < 2; j++) {
            for (int i = 1000000; i < 1000100; i++) {
                assertThat(tablet.get("com.amazon", String.format("%05d", i) + "site"), is(Optional.of("data data data data dara data data data" + i)));
             //   assertThat(tablet.get("com.google", String.format("%05d", i) + "page"), is(Optional.of("data data data data dara data data data" + i)));
            }
        }
    }
}