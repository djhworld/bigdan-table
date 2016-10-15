package io.github.djhworld.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.stream.Stream;

public interface Source {
    InputStream open() throws IOException;
    InputStream getRange(int offset, int length) throws IOException;
    Path getLocation();
}
