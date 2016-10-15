package io.github.djhworld.io;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class FileSink implements Sink {
    private final Path location;

    public FileSink(Path location) {
        this.location = location;
    }

    @Override
    public void flush(ByteBuffer buffer) throws IOException {
        try(FileChannel fileChannel = new FileOutputStream(location.toFile(), false).getChannel()) {
            fileChannel.write(buffer);
        }
    }
}