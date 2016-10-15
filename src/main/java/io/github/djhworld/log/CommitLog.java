package io.github.djhworld.log;

import io.github.djhworld.log.exception.CommitLogException;
import io.github.djhworld.model.RowMutation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static io.github.djhworld.model.RowMutation.deserialise;

public class CommitLog implements Iterable<RowMutation> {
    private static final byte[] NEWLINE = "\n".getBytes();
    private final Path location;

    public CommitLog(Path location) {
        this.location = location;
    }

    public synchronized void commit(RowMutation rowMutation) throws IOException {
        try (FileOutputStream outputStream = new FileOutputStream(location.toFile(), true)) {
            outputStream.write(
                    rowMutation.serialise()
            );
            outputStream.write(NEWLINE);
            outputStream.flush();
        }
    }

    public boolean exists() {
        return Files.exists(location);
    }

    @Override
    public Iterator<RowMutation> iterator() {
        Iterator<RowMutation> iterator = new Iterator<RowMutation>() {
            String currentLine;
            BufferedReader reader = getReader();

            @Override
            public boolean hasNext() {
                try {
                    currentLine = reader.readLine();
                    if (currentLine == null) {
                        reader.close();
                        return false;
                    }
                } catch (IOException e) {
                    throw new CommitLogException(e);
                }

                return true;
            }

            @Override
            public RowMutation next() {
                return deserialise(currentLine);
            }

            public BufferedReader getReader() {
                try {
                    return new BufferedReader(new InputStreamReader(new FileInputStream(location.toFile())));
                } catch (IOException e) {
                    throw new CommitLogException(e);
                }
            }
        };

        return iterator;
    }
}
