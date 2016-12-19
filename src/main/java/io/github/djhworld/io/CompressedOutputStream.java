package io.github.djhworld.io;

import java.io.IOException;
import java.io.OutputStream;

public abstract class CompressedOutputStream extends OutputStream {
    //finish() method is what caused this mess
    public abstract void finish() throws IOException;

    @Override
    public abstract void write(int b) throws IOException;

    @Override
    public abstract void write(byte[] b) throws IOException;

    @Override
    public abstract void write(byte[] b, int off, int len) throws IOException;

    @Override
    public abstract void flush() throws IOException;

    @Override
    public abstract void close() throws IOException;

}
