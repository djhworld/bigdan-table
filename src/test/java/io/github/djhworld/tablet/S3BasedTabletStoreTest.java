package io.github.djhworld.tablet;

import com.amazonaws.services.s3.AmazonS3Client;
import io.github.djhworld.log.CommitLog;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.file.Paths;
import java.util.Optional;

import static io.github.djhworld.io.CompressionType.GZIP;
import static io.github.djhworld.io.CompressionType.SNAPPY;
import static io.github.djhworld.io.CompressionType.UNCOMPRESSED;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@Ignore
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
        when(mockedTabletMetadataService.getCompressionCodecFor(eq("id"))).thenReturn(GZIP);


        Tablet tablet = new Tablet("id", mockedTabletMetadataService);

        for (int j = 0; j < 4; j++) {
            for (int i = 0; i < 123456; i++) {
                tablet.apply(newAddMutation("com.amazon", String.format("%05d", i) + "site", "data data data data dara data data data" + i));
            }
            tablet.flush();
        }
        when(mockedTabletMetadataService.getCurrentTabletGeneration(eq("id"))).thenReturn(1).thenReturn(2);
        tablet.compact();
        for (int i = 0; i < 123456; i++) {
            assertThat(tablet.get("com.amazon", String.format("%05d", i) + "site"), is(Optional.of("data data data data dara data data data" + i)));
        }
    }
}