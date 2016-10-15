package io.github.djhworld.io;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static java.nio.channels.Channels.newInputStream;

public class FileSource implements Source {
    private static final String READ_ONLY = "r";
    private final Path location;

    public FileSource(Path location) {
        this.location = location;
    }

    @Override
    public InputStream open() throws IOException {
        return new FileInputStream(location.toFile());
    }

    @Override
    public InputStream getRange(int offset, int length) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(location.toFile(), READ_ONLY);
        randomAccessFile.seek(offset);

        return new BoundedInputStream(
                newInputStream(randomAccessFile.getChannel()),
                length);
    }


    @Override
    public Path getLocation() {
        return location;
    }
}
