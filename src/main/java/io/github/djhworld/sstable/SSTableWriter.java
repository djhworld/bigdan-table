package io.github.djhworld.sstable;

import io.github.djhworld.io.*;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;

import static io.github.djhworld.sstable.SSTable.Footer;
import static io.github.djhworld.sstable.SSTable.Header;
import static io.github.djhworld.sstable.SSTable.Header.HEADER_LENGTH;

public class SSTableWriter implements Closeable {
    private static final int VERSION = 1;
    private static final int DEFAULT_BLOCK_SIZE = 64000;
    private final DataOutputStream dos;
    private final RewindableByteArrayOutputStream rbaos;
    private final SSTable.Footer footer;
    private final Sink sink;
    private final CompressionStrategy compressionStrategy;
    private final int blockSize;

    private int currentBlockNo;
    private WriteableBlock currentBlock;

    public SSTableWriter(Sink sink, CompressionType compressionType) throws IOException {
        this(sink, compressionType, DEFAULT_BLOCK_SIZE);
    }

    public SSTableWriter(Sink sink, CompressionType compressionType, int blockSize) throws IOException {
        this.sink = sink;
        this.compressionStrategy = CompressionStrategyFactory.create(compressionType);
        this.blockSize = blockSize;
        this.rbaos = new RewindableByteArrayOutputStream();
        this.dos = new DataOutputStream(rbaos);
        this.currentBlockNo = 0;
        this.currentBlock = newBlock();
        this.footer = new Footer(this.compressionStrategy);
        writeDummyHeader(dos);
    }

    /**
     * Keys will be inserted into the footer
     */
    public void write(String rowKey, String columnKey, String value, long timestamp) throws IOException {
        if (!currentBlock.hasEnoughSpaceFor(value)) {
            flushCurrentBlock();
            this.currentBlock = newBlock();
        }

        int blockOffset = this.currentBlock.put(value);
        this.footer.putEntry(
                rowKey,
                columnKey,
                BlockEntryDescriptor.of(this.currentBlockNo, timestamp, blockOffset));
    }

    @Override
    public void close() throws IOException {
        flushCurrentBlock();
        int footerStartOffset = getNoOfBytesWritten();
        int footerUncompressedLength = footer.writeTo(dos);

        //rewind to the beginning but get current length
        int length = rbaos.rewind();

        Header header = new Header(
                VERSION,
                compressionStrategy.getCompressionType(),
                currentBlockNo,
                this.blockSize,
                footerStartOffset,
                footerUncompressedLength,
                getNoOfBytesWritten()
        );
        header.writeTo(dos);

        sink.flush(rbaos.toInputStream(), length);
    }

    private int getNoOfBytesWritten() {
        return dos.size();
    }

    private void writeDummyHeader(DataOutputStream dos) throws IOException {
        byte[] headerBytes = new byte[HEADER_LENGTH];
        dos.write(headerBytes);
    }

    private WriteableBlock newBlock() {
        return new WriteableBlock(DEFAULT_BLOCK_SIZE);
    }

    private void flushCurrentBlock() throws IOException {
        int blockStart = getNoOfBytesWritten();

        CompressedOutputStream compressedOutputStream = compressionStrategy.newOutputStream(dos);
        currentBlock.flushTo(compressedOutputStream);
        compressedOutputStream.finish();


        int blockEnd = getNoOfBytesWritten();
        int blockLength = blockEnd - blockStart;

        this.footer.putBlockDescriptor(
                new BlockDescriptor(
                        blockStart,
                        blockLength
                )
        );

        currentBlockNo++;
    }
}
