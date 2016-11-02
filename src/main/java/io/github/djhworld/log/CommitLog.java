package io.github.djhworld.log;

import io.github.djhworld.log.exception.CommitLogException;
import io.github.djhworld.model.RowMutation;
import org.slf4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.github.djhworld.model.RowMutation.deserialise;
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.slf4j.LoggerFactory.getLogger;

public class CommitLog implements Iterable<RowMutation>, Closeable {
    private static final Logger LOGGER = getLogger(CommitLog.class);
    private static final byte[] NEWLINE = "\n".getBytes();
    private final Path location;
    private OutputStream outputStream;
    ReentrantReadWriteLock.WriteLock writeLock;
    ReentrantReadWriteLock.ReadLock readLock;


    public CommitLog(Path location) throws IOException {
        this.location = location;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        open();
        writeLock = lock.writeLock();
        readLock = lock.readLock();
    }

    public void commit(RowMutation rowMutation) throws IOException {
        try {
            writeLock.lock();
            outputStream.write(
                    rowMutation.serialise() //more efficient way of doing this?
            );
            outputStream.write(NEWLINE);
            outputStream.flush();
        } finally {
            writeLock.unlock();
        }
    }

    public void checkpoint() throws IOException {
        try {
            writeLock.lock();
            LOGGER.info("Checkpointing commit log");
            close();
            Files.deleteIfExists(location); // TODO: what if this fails?
            open();
        } finally {
            writeLock.unlock();
        }
    }

    public boolean exists() {
        return Files.exists(location);
    }

    @Override
    public Iterator<RowMutation> iterator() {
        readLock.lock();
        Iterator<RowMutation> iterator = new Iterator<RowMutation>() {
            String currentLine;
            BufferedReader reader = getReader();

            @Override
            public boolean hasNext() {
                try {
                    currentLine = reader.readLine();
                    if (currentLine == null) {
                        try {
                            reader.close();
                        } finally {
                            readLock.unlock();
                        }
                        return false;
                    }
                } catch (IOException e) {
                    readLock.unlock();
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
    public void close() {
        closeQuietly(outputStream);
    }

    private void open() throws IOException {
        LOGGER.info("Loading commit log at " + location);
        this.outputStream = new FileOutputStream(location.toFile(), true);
    }
}
