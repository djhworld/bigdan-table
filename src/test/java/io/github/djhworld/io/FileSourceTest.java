package io.github.djhworld.io;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.google.common.io.Resources.getResource;
import static io.github.djhworld.sstable.SSTableTest.TEST_DATA_DB;
import static java.nio.file.Paths.get;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class FileSourceTest {
    private Path path;
    private FileSource fileSource;

    @Before
    public void setUp() throws Exception {
        path = get(getResource("file-source.txt").toURI());
        fileSource = new FileSource(path);
    }

    @Test
    public void shouldProvideFullInputStream() throws Exception {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(fileSource.open()))) {
            reader.lines().forEach(line -> {
                assertThat(line, is("test data"));
            });
        }
    }

    @Test
    public void shouldProvideRangedInputStream() throws Exception {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(fileSource.getRange(5, 4)))) {
            reader.lines().forEach(line -> {
                assertThat(line, is("data"));
            });
        }
    }

    @Test
    public void shouldProvideLocation() {
        assertThat(fileSource.getLocation(), is(path));
    }
}