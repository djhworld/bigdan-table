package io.github.djhworld.sstable;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.TreeBasedTable;
import io.github.djhworld.exception.SSTableException;
import io.github.djhworld.io.Source;
import io.github.djhworld.model.RowMutation;
import org.slf4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.collect.TreeBasedTable.create;
import static io.github.djhworld.model.RowMutation.newAddMutation;
import static io.github.djhworld.sstable.SSTable.Header.HEADER_LENGTH;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.slf4j.LoggerFactory.getLogger;

public class SSTable {
    private static final Logger LOGGER = getLogger(SSTable.class);
    private static final int MAGIC = 55748130;

    private final LoadingCache<Integer, Block> blockCache;
    private final Source source;
    private final Header header;
    private final Footer footer;

    public final String minKey;
    public final String maxKey;

    public SSTable(Source source) {
        LOGGER.info("Initialising SSTable at " + source.getLocation());
        try {
            this.source = source;
            this.header = new Header(this.source);
            this.footer = newFooter();
            this.blockCache = newBlockCache(this.header.noOfBlocks);

            this.minKey = this.footer.keysToBlocksIndex.rowMap().firstKey();
            this.maxKey = this.footer.keysToBlocksIndex.rowMap().lastKey();
        } catch (Exception e) {
            throw new SSTableException("Exception caught attempting to initialise SSTable", e);
        }
    }

    public boolean contains(String rowName, String columnName) {
        return this.footer.keysToBlocksIndex.contains(
                rowName, columnName
        );
    }

    public Optional<String> get(String rowName, String columnName) {
        BlockDescriptor blockDescriptor = this.footer.keysToBlocksIndex.get(rowName, columnName);

        if (blockDescriptor == null)
            return empty();

        return ofNullable(getValueFromBlock(blockDescriptor));
    }

    public void scan(Consumer<RowMutation> consumer) {
        this.loadAllBlocksIntoCache();
        this.footer.keysToBlocksIndex.cellSet().forEach((cell) -> {
            String value = getValueFromBlock(cell.getValue());
            consumer.accept(newAddMutation(cell.getRowKey(), cell.getColumnKey(), value));
        });
    }

    public long cachedBlocks() { return blockCache.size(); }

    public int blocks() {
        return header.noOfBlocks;
    }

    public int blockSize() {
        return header.blockSize;
    }

    public int noOfRows() {
        return this.footer.keysToBlocksIndex.size();
    }

    private Footer newFooter() throws IOException {
        int length = this.header.fileLength - this.header.footerOffset;
        int offset = this.header.fileLength - length;
        return new Footer(this.source, offset, length);
    }

    private LoadingCache<Integer, Block> newBlockCache(int blockCacheSize) {
        return newBuilder()
                .maximumSize(blockCacheSize)
                .build(
                        new CacheLoader<Integer, Block>() {
                            public Block load(Integer blockNo) throws IOException {
                                return getBlock(blockNo);
                            }
                        });
    }

    private String getValueFromBlock(BlockDescriptor blockDescriptor) {
        try {
            Block block = this.blockCache.get(blockDescriptor.blockNumber);
            return block.read(blockDescriptor.blockOffset);
        } catch (ExecutionException e) {
            throw new SSTableException("Problem reading from block " + blockDescriptor.blockNumber, e.getCause());
        }
    }

    private Block getBlock(int blockNo) throws IOException {
        int offset = HEADER_LENGTH;

        if (blockNo > 0)
            offset = offset + (blockNo * this.blockSize());

        try (DataInputStream inputStream = new DataInputStream(source.getRange(offset, this.blockSize()))) {
            byte[] block = new byte[this.blockSize()];
            inputStream.readFully(block);
            return new ReadOnlyBlock(block);
        }
    }

    private void loadAllBlocksIntoCache() {
        int totalBytesForAllBlocks = this.blockSize() * this.header.noOfBlocks;
        LOGGER.info("Retrieving " + totalBytesForAllBlocks + " bytes of data from source to populate block cache");
        try (DataInputStream inputStream = new DataInputStream(source.getRange(HEADER_LENGTH, totalBytesForAllBlocks))) {
            for (int blockNo = 0; blockNo < this.header.noOfBlocks; blockNo++) {
                byte[] block = new byte[this.blockSize()];
                inputStream.readFully(block);
                this.blockCache.put(blockNo, new ReadOnlyBlock(block));
                block = null;
            }
        } catch (IOException e) {
            throw new SSTableException("Error attempting to load all blocks from cache", e);
        }
    }

    static class Header {
        static final int HEADER_LENGTH = 24;
        final int magic;
        final int version;
        final int footerOffset;
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
                this.noOfBlocks = dis.readInt();
                this.blockSize = dis.readInt();
                this.footerOffset = dis.readInt();
                this.fileLength = dis.readInt();
            }
        }

        Header(int version, int noOfBlocks, int blockSize, int footerOffset, int fileLength) {
            this.magic = MAGIC;
            this.version = version;
            this.noOfBlocks = noOfBlocks;
            this.blockSize = blockSize;
            this.footerOffset = footerOffset;
            this.fileLength = fileLength;
        }
    }

    static class Footer {
        private static final int INDEX_ENTRY_METADATA_BYTES = 12;
        private static final String ENTRY_ROW_KEY_SEPARATOR = "|";
        private final TreeBasedTable<String, String, BlockDescriptor> keysToBlocksIndex;

        Footer() {
            this.keysToBlocksIndex = create();
        }

        private Footer(Source source, int footerOffset, int length) throws IOException, SSTableException {
            this();
            try (DataInputStream inputStream = new DataInputStream(source.getRange(footerOffset, length))) {
                LOGGER.info("Initialising SSTable footer at offset " + footerOffset);
                int currentPos = 0;

                while (currentPos < length) {
                    int bytesRead = readBlockIndexEntry(inputStream);
                    currentPos += bytesRead;
                }
            }
        }

        void put(String rowName, String columnName, BlockDescriptor blockDescriptor) {
            this.keysToBlocksIndex.put(rowName, columnName, blockDescriptor);
        }

        /**
         * @param dos
         * @throws SSTableException
         */
        public void writeTo(DataOutputStream dos) {
            this.keysToBlocksIndex.cellSet().forEach((cell) -> {
                try {
                    byte[] keyBytes = Joiner.on(ENTRY_ROW_KEY_SEPARATOR).join(cell.getRowKey(), cell.getColumnKey()).getBytes();
                    int blockIndexEntryLength = keyBytes.length + INDEX_ENTRY_METADATA_BYTES;

                    dos.writeInt(blockIndexEntryLength);
                    dos.write(keyBytes);
                    dos.writeInt(cell.getValue().blockNumber);
                    dos.writeInt(cell.getValue().blockOffset);
                } catch (IOException e) {
                    throw new SSTableException("Error writing to footer", e);
                }
            });
        }

        private int readBlockIndexEntry(DataInputStream inputStream) throws IOException {
            int blockIndexEntryLength = inputStream.readInt();

            if (blockIndexEntryLength <= 0)
                throw new SSTableException("Footer is corrupt, cannot read block index entry");

            byte[] keyBytes = new byte[blockIndexEntryLength - INDEX_ENTRY_METADATA_BYTES];
            inputStream.readFully(keyBytes);

            int valueBlockNumber = inputStream.readInt();
            int valueBlockOffset = inputStream.readInt();

            Iterator<String> rowKey = Splitter.on(ENTRY_ROW_KEY_SEPARATOR).split(new String(keyBytes)).iterator();
            this.keysToBlocksIndex.put(
                    rowKey.next(),
                    rowKey.next(),
                    BlockDescriptor.of(valueBlockNumber, valueBlockOffset));

            return blockIndexEntryLength;
        }


    }
}
