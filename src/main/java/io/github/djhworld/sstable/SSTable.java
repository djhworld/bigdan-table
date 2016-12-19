package io.github.djhworld.sstable;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import io.github.djhworld.exception.SSTableException;
import io.github.djhworld.io.CompressionCodec;
import io.github.djhworld.io.Compressor;
import io.github.djhworld.io.CompressorFactory;
import io.github.djhworld.io.Source;
import io.github.djhworld.model.RowMutation;
import org.slf4j.Logger;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.TreeBasedTable.create;
import static io.github.djhworld.io.CompressionCodec.*;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static io.github.djhworld.sstable.SSTable.Header.HEADER_LENGTH;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.slf4j.LoggerFactory.getLogger;

public class SSTable {
    private static final Logger LOGGER = getLogger(SSTable.class);
    private static final int MAGIC = 55748130;

    private final Source source;
    private final Header header;
    private final Footer footer;
    private final Block[] blocks;
    private final Compressor compressor;

    public SSTable(Source source) {
        LOGGER.info("Initialising SSTable at " + source.getLocation());
        try {
            this.source = source;
            this.header = new Header(this.source);
            this.compressor = CompressorFactory.create(header.compressionCodec);
            this.footer = newFooter();

            if (header.noOfBlocks != this.footer.blockDescriptors.size())
                throw new SSTableException("Number of blocks do not match in header and footer");

            this.blocks = new ReadOnlyBlock[header.noOfBlocks];
        } catch (Exception e) {
            throw new SSTableException("Exception caught attempting to initialise SSTable", e);
        }
    }

    public boolean contains(String rowName, String columnName) {
        return this.footer.keysToBlockEntries.contains(
                rowName, columnName
        );
    }

    //TODO: get all versions?
    public Optional<String> get(String rowName, String columnName) {
        List<BlockEntryDescriptor> blockEntryDescriptors =
                this.footer.keysToBlockEntries.get(rowName, columnName);

        if (blockEntryDescriptors == null || blockEntryDescriptors.size() == 0)
            return empty();

        return ofNullable(getValueFromBlock(blockEntryDescriptors.get(0)));
    }

    public Stream<RowMutation> stream() {
        this.loadAllBlocks();
        Iterator<RowMutation> scanIterator = createScanIterator();
        return StreamSupport.stream(spliteratorUnknownSize(scanIterator, Spliterator.SUBSIZED), false);
    }

    public Stream<RowMutation> stream(String rowKey) {
        return scanRowFor(rowKey, "");
    }

    public Stream<RowMutation> stream(String rowKey, String columnFamily) {
        return scanRowFor(rowKey, columnFamily + ":");
    }

    public long cachedBlocks() {
        int size = 0;
        for (int i = 0; i < blocks.length; i++) {
            if (blocks[i] != null)
                size++;
        }
        return size;
    }

    public int blocks() {
        return header.noOfBlocks;
    }

    public int blockSize() {
        return header.blockSize;
    }

    public int noOfRows() {
        return this.footer.keysToBlockEntries.size();
    }

    private Stream<RowMutation> scanRowFor(String rowKey, String columnFamily) {
        SortedMap<String, List<BlockEntryDescriptor>> row = this.footer.keysToBlockEntries.row(rowKey);

        if (row == null)
            return Stream.empty();

        Iterator<RowMutation> rowMutationIterator = createRowColumnFamilyIterator(rowKey, columnFamily, row);
        return StreamSupport.stream(spliteratorUnknownSize(rowMutationIterator, Spliterator.SUBSIZED), false);
    }

    private Iterator<RowMutation> createScanIterator() {
        return new Iterator<RowMutation>() {
            private Iterator<Table.Cell<String, String, List<BlockEntryDescriptor>>> cellIterator = footer.keysToBlockEntries.cellSet().iterator();
            private Iterator<BlockEntryDescriptor> valueVersionIterator;
            private Table.Cell<String, String, List<BlockEntryDescriptor>> currentCell;
            private BlockEntryDescriptor currentValueVersion;

            @Override
            public boolean hasNext() {
                if (!cellIterator.hasNext())
                    return false;

                if (valueVersionIterator == null || !valueVersionIterator.hasNext()) {
                    currentCell = cellIterator.next();
                    valueVersionIterator = currentCell.getValue().iterator();
                    currentValueVersion = valueVersionIterator.next();
                } else if (valueVersionIterator.hasNext()) {
                    currentValueVersion = valueVersionIterator.next();
                }

                return true;
            }

            @Override
            public RowMutation next() {
                String value = getValueFromBlock(currentValueVersion);
                return newAddMutation(
                        currentCell.getRowKey(),
                        currentCell.getColumnKey(),
                        value,
                        currentValueVersion.timestamp
                );
            }
        };
    }

    private Iterator<RowMutation> createRowColumnFamilyIterator(final String rowKey,
                                                                final String columnFamily,
                                                                final SortedMap<String, List<BlockEntryDescriptor>> row) {
        Preconditions.checkNotNull(row);

        return new Iterator<RowMutation>() {
            private boolean columnFamilyFound = false;
            private Iterator<Map.Entry<String, List<BlockEntryDescriptor>>> columnsIterator = row.entrySet().iterator();
            private Iterator<BlockEntryDescriptor> valueVersionIterator;
            private Map.Entry<String, List<BlockEntryDescriptor>> currentColumn;
            private BlockEntryDescriptor currentValueVersion;

            @Override
            public boolean hasNext() {
                if (!columnsIterator.hasNext())
                    return false;

                moveForward();

                // column family out of scope so we can quit
                if (columnFamilyFound && !currentColumn.getKey().startsWith(columnFamily))
                    return false;

                //skip until column family found
                while (!columnFamilyFound && columnsIterator.hasNext()) {
                    if (currentColumn.getKey().startsWith(columnFamily))
                        columnFamilyFound = true;
                    else
                        moveForward();
                }

                return columnFamilyFound;
            }

            private void moveForward() {
                if (valueVersionIterator == null || !valueVersionIterator.hasNext()) {
                    currentColumn = columnsIterator.next();
                    valueVersionIterator = currentColumn.getValue().iterator();
                    currentValueVersion = valueVersionIterator.next();
                } else if (valueVersionIterator.hasNext()) {
                    currentValueVersion = valueVersionIterator.next();
                }
            }

            @Override
            public RowMutation next() {
                String value = getValueFromBlock(currentValueVersion);
                return newAddMutation(
                        rowKey,
                        currentColumn.getKey(),
                        value,
                        currentValueVersion.timestamp);
            }
        };
    }

    private Footer newFooter() throws IOException {
        int footerCompressedLength = this.header.fileLength - this.header.footerOffset;
        int footerOffset = this.header.fileLength - footerCompressedLength;
        return new Footer(
                this.source,
                this.compressor,
                footerOffset,
                footerCompressedLength,
                this.header.footerUncompressedLength
        );
    }

    private String getValueFromBlock(BlockEntryDescriptor blockEntryDescriptor) {
        try {
            Block block = getBlock(blockEntryDescriptor.id);
            return block.read(blockEntryDescriptor.offset);
        } catch (IOException e) {
            throw new SSTableException("Problem reading from block " + blockEntryDescriptor.id, e.getCause());
        }
    }

    private Block getBlock(int blockId) throws IOException {
        if (blocks[blockId] == null)
            blocks[blockId] = loadBlock(blockId);

        return blocks[blockId];
    }

    private Block loadBlock(int blockId) throws IOException {
        BlockDescriptor blockDescriptor = this.footer.getBlockDescriptor(blockId);

        try (DataInputStream inputStream = new DataInputStream(compressor.newCompressedInputStream(source.getRange(blockDescriptor.offset, blockDescriptor.length)))) {
            byte[] block = new byte[this.blockSize()];
            inputStream.readFully(block);
            return new ReadOnlyBlock(block);
        }
    }

    private void loadAllBlocks() {
        if (cachedBlocks() == header.noOfBlocks)
            return;

        int totalBytesForAllBlocks = this.footer.computeLengthOfAllBlocks();

        byte[] uncompressedBlock = new byte[header.blockSize];
        byte[] allBlocks = new byte[totalBytesForAllBlocks];
        try (DataInputStream allBlocksStream = new DataInputStream(source.getRange(HEADER_LENGTH, totalBytesForAllBlocks))) {
            allBlocksStream.readFully(allBlocks);

            int blockNo = 0;
            for (BlockDescriptor blockDescriptor : this.footer.blockDescriptors) {
                if (blocks[blockNo] == null) {
                    DataInputStream dis = new DataInputStream(
                            compressor.newCompressedInputStream(
                                    new ByteArrayInputStream(
                                            allBlocks,
                                            blockDescriptor.offset - HEADER_LENGTH,
                                            blockDescriptor.length)
                            )
                    );

                    dis.readFully(uncompressedBlock);
                    blocks[blockNo] = new ReadOnlyBlock(uncompressedBlock);
                }
                blockNo++;
            }
        } catch (IOException e) {
            throw new SSTableException("Error attempting to load all blocks from cache", e);
        }
    }

    static class Header {
        static final int HEADER_LENGTH = 29;
        final int magic;
        final int version;
        final CompressionCodec compressionCodec;
        final int footerOffset;
        final int footerUncompressedLength;
        final int fileLength;
        final int noOfBlocks;
        final int blockSize;

        private Header(Source source) throws IOException {
            LOGGER.info("Initialising SSTable header");
            try (DataInputStream dis = new DataInputStream(source.getRange(0, HEADER_LENGTH))) {
                this.magic = dis.readInt();

                if (this.magic != MAGIC)
                    throw new SSTableException("Header does not conform to specification");

                this.version = dis.readInt();
                this.compressionCodec = valueOf(dis.readByte());
                this.noOfBlocks = dis.readInt();
                this.blockSize = dis.readInt();
                this.footerOffset = dis.readInt();
                this.footerUncompressedLength = dis.readInt();
                this.fileLength = dis.readInt();
            }
        }

        Header(int version, CompressionCodec compressionCodec, int noOfBlocks, int blockSize, int footerOffset, int footerUncompressedLength, int fileLength) {
            this.magic = MAGIC;
            this.version = version;
            this.compressionCodec = compressionCodec;
            this.noOfBlocks = noOfBlocks;
            this.blockSize = blockSize;
            this.footerOffset = footerOffset;
            this.footerUncompressedLength = footerUncompressedLength;
            this.fileLength = fileLength;
        }

        /**
         * @param dos the outputstream to write to
         * @throws SSTableException
         */
        void writeTo(DataOutputStream dos) {
            try {
                dos.writeInt(this.magic);
                dos.writeInt(this.version);
                dos.writeByte(this.compressionCodec.getId());
                dos.writeInt(this.noOfBlocks);
                dos.writeInt(this.blockSize);
                dos.writeInt(this.footerOffset);
                dos.writeInt(this.footerUncompressedLength);
                dos.writeInt(this.fileLength);
            } catch (IOException e) {
                throw new SSTableException("Error writing header", e);
            }
        }
    }

    static class Footer {
        private static final int BLOCK_DESCRIPTOR_HEADER_BYTES = 4;
        private static final int BLOCK_DESCRIPTOR_BYTES = 8;
        private static final int BLOCK_ENTRY_METADATA_BYTES = 20;
        private static final String ENTRY_ROW_KEY_SEPARATOR = "|";
        private static final Splitter ENTRY_ROW_KEY_SPLITTER = Splitter.on(ENTRY_ROW_KEY_SEPARATOR);
        private static final Joiner ENTRY_ROW_KEY_JOINER = Joiner.on(ENTRY_ROW_KEY_SEPARATOR);
        private final List<BlockDescriptor> blockDescriptors;
        private final TreeBasedTable<String, String, List<BlockEntryDescriptor>> keysToBlockEntries;
        private final Compressor compressor;

        Footer(Compressor compressor) {
            this.compressor = compressor;
            this.blockDescriptors = newArrayList();
            this.keysToBlockEntries = create();
        }


        private Footer(Source source, Compressor compressor, int footerOffset, int compressedLength, int uncompressedLength) throws IOException, SSTableException {
            this(compressor);
            try (DataInputStream inputStream = new DataInputStream(compressor.newCompressedInputStream(source.getRange(footerOffset, compressedLength)))) {
                LOGGER.info("Initialising SSTable footer at offset " + footerOffset);
                int currentPos = 0;
                currentPos += loadBlockDescriptors(inputStream);

                while (currentPos < uncompressedLength) {
                    int bytesRead = readBlockEntry(inputStream);
                    currentPos += bytesRead;
                }
            }
        }

        private int loadBlockDescriptors(DataInputStream inputStream) throws IOException {
            int bytesRead = 0;
            int noOfBlockDescriptors = inputStream.readInt();

            bytesRead += BLOCK_DESCRIPTOR_HEADER_BYTES;
            for (int descriptorsRead = 0; descriptorsRead < noOfBlockDescriptors; descriptorsRead++) {
                this.blockDescriptors.add(new BlockDescriptor(
                        inputStream.readInt(),
                        inputStream.readInt()
                ));
                bytesRead += BLOCK_DESCRIPTOR_BYTES;
            }

            return bytesRead;
        }

        void putEntry(String rowName, String columnName, BlockEntryDescriptor blockEntryDescriptor) {
            List<BlockEntryDescriptor> blockEntryDescriptors = this.keysToBlockEntries.get(rowName, columnName);
            if (blockEntryDescriptors == null) {
                blockEntryDescriptors = newArrayList();
                this.keysToBlockEntries.put(rowName, columnName, blockEntryDescriptors);
            }
            blockEntryDescriptors.add(blockEntryDescriptor);
        }

        void putBlockDescriptor(BlockDescriptor blockDescriptor) {
            this.blockDescriptors.add(blockDescriptor);
        }

        BlockDescriptor getBlockDescriptor(int blockId) {
            // cannot pass a block id >= number of descriptors
            if (blockId < 0 || blockId >= blockDescriptors.size())
                throw new IllegalArgumentException("Requested block id: " + blockId + " is invalid");


            return blockDescriptors.get(blockId);
        }

        int computeLengthOfAllBlocks() {
            return blockDescriptors
                    .stream()
                    .mapToInt(bd -> bd.length)
                    .sum();
        }

        /**
         * @throws SSTableException
         */
        int writeTo(OutputStream out) {
            try (DataOutputStream compressedOut = new DataOutputStream(compressor.newCompressedOutputStream(out))) {
                writeBlockDescriptors(compressedOut);
                writeBlockEntries(compressedOut);
                return compressedOut.size();
            } catch (IOException e) {
                throw new SSTableException("Error writing to footer", e);
            }
        }

        /**
         * [ no-of-descriptors ][ offset ][ length ][ offset ][ length ]...
         * <---------4---------><--------8---------><--------8--------->
         *
         * @param out the output stream to write the block descriptors to
         * @throws IOException
         */
        private void writeBlockDescriptors(DataOutputStream out) throws IOException {
            out.writeInt(blockDescriptors.size());
            this.blockDescriptors.forEach(bd -> {
                try {
                    out.writeInt(bd.offset);
                    out.writeInt(bd.length);
                } catch (IOException e) {
                    throw new SSTableException("Error writing block descriptor to footer", e);
                }
            });
        }

        /**
         * Each entry is stored like so
         * <p>
         * [entry-length][     key     ][block-id][block-offset]
         * <---4--------><------n------><---4----><-----4------>
         * <p>
         * The entry length describes the length of the full entry
         */
        private void writeBlockEntries(DataOutputStream out) {
            this.keysToBlockEntries.cellSet().forEach((cell) -> {
                try {
                    for (BlockEntryDescriptor entryDescriptor : cell.getValue()) {
                        byte[] keyBytes = ENTRY_ROW_KEY_JOINER.join(cell.getRowKey(), cell.getColumnKey()).getBytes();
                        int blockIndexEntryLength = keyBytes.length + BLOCK_ENTRY_METADATA_BYTES;
                        out.writeInt(blockIndexEntryLength);
                        out.write(keyBytes);
                        out.writeInt(entryDescriptor.id);
                        out.writeLong(entryDescriptor.timestamp);
                        out.writeInt(entryDescriptor.offset);
                    }
                } catch (IOException e) {
                    throw new SSTableException("Error writing entry to footer", e);
                }
            });
        }

        private int readBlockEntry(DataInputStream inputStream) throws IOException {
            int blockEntryLength = inputStream.readInt();

            if (blockEntryLength <= 0)
                throw new SSTableException("Footer is corrupt, cannot read block index entry");

            byte[] keyBytes = new byte[blockEntryLength - BLOCK_ENTRY_METADATA_BYTES];
            inputStream.readFully(keyBytes);

            int blockId = inputStream.readInt();
            long timestamp = inputStream.readLong();
            int blockOffset = inputStream.readInt();

            Iterator<String> rowKey = ENTRY_ROW_KEY_SPLITTER.split(new String(keyBytes)).iterator();

            this.putEntry(rowKey.next(), rowKey.next(), BlockEntryDescriptor.of(blockId, timestamp, blockOffset));
            return blockEntryLength;
        }
    }
}