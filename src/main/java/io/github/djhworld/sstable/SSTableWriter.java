package io.github.djhworld.sstable;

import io.github.djhworld.io.RewindableByteArrayOutputStream;
import io.github.djhworld.io.Sink;
import io.github.djhworld.model.RowMutation;

import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.IOException;

import static io.github.djhworld.sstable.SSTable.Footer;
import static io.github.djhworld.sstable.SSTable.Header;
import static io.github.djhworld.sstable.SSTable.Header.HEADER_LENGTH;

public class SSTableWriter implements Closeable {
    private static final int VERSION = 1;
    private static final int DEFAULT_BLOCK_SIZE = 65_536;
    private final DataOutputStream dos;
    private final RewindableByteArrayOutputStream cos;
    private final SSTable.Footer footer;
    private final Sink sink;
    private final int blockSize;

    private int currentBlockNo;
    private WriteableBlock currentBlock;

    public SSTableWriter(Sink sink) throws IOException {
        this(sink, DEFAULT_BLOCK_SIZE);
    }

    public SSTableWriter(Sink sink, int blockSize) throws IOException {
        this.sink = sink;
        this.blockSize = blockSize;
        this.cos = new RewindableByteArrayOutputStream();
        this.dos = new DataOutputStream(cos);
        this.currentBlockNo = 0;
        this.currentBlock = newBlock();
        this.footer = new Footer();
        writeDummyHeader(dos);
    }

    /**
     * Keys will be inserted into the footer
     */
    public void write(RowMutation rowMutation) throws IOException {
        if (!currentBlock.hasEnoughSpaceFor(rowMutation.value)) {
            flushCurrentBlock();
            this.currentBlock = newBlock();
        }

        int blockOffset = this.currentBlock.put(rowMutation.value);
        this.footer.put(rowMutation.rowKey, rowMutation.columnKey, BlockDescriptor.of(this.currentBlockNo, blockOffset));
    }

    @Override
    public void close() throws IOException {
        flushCurrentBlock();
        int footerOffset = flushFooter();

        //rewind to the beginning but get current length
        int length = cos.rewind();

        Header header = new Header(
                VERSION,
                currentBlockNo,
                this.blockSize,
                footerOffset,
                getNoOfBytesWritten()
        );
        header.writeTo(dos);

        sink.flush(cos.toInputStream(), length);
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
        currentBlock.flushTo(dos);
        currentBlockNo++;
    }

    /**
     * Each entry is stored in the footer like so
     * <p>
     * [length][     key     ][block-number][block-offset]
     * <---4----------n------------4--------------4------>
     * <-----------------------length-------------------->
     *
     * @return byte position of where the footer starts
     */
    private int flushFooter() throws IOException {
        int footerStartOffset = getNoOfBytesWritten();
        footer.writeTo(dos);
        return footerStartOffset;
    }
}
