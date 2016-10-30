package io.github.djhworld.log;

import io.github.djhworld.log.exception.CommitLogException;
import io.github.djhworld.model.RowMutation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

import static io.github.djhworld.model.RowMutation.deserialise;
import static org.apache.commons.io.IOUtils.closeQuietly;

public class CommitLog implements Iterable<RowMutation>, Closeable {
    private static final byte[] NEWLINE = "\n".getBytes();
    private final Path location;
    private OutputStream outputStream;

    public CommitLog(Path location) throws IOException {
        this.location = location;
        this.outputStream = new FileOutputStream(location.toFile(), true);
    }

    public synchronized void commit(RowMutation rowMutation) throws IOException {
        //TODO: check if output is still open?
        outputStream.write(
                rowMutation.serialise() //more efficient way of doing this?
        );
        outputStream.write(NEWLINE);
        outputStream.flush();
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

    @Override
    public void close() throws IOException {
        closeQuietly(outputStream);
    }
}
