package io.github.djhworld.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class FileSink implements Sink {
    private final Path location;

    public FileSink(Path location) {
        this.location = location;
    }

    @Override
    public void flush(InputStream inputStream, int length) throws IOException {
        Files.copy(inputStream, location, StandardCopyOption.REPLACE_EXISTING);
    }
}