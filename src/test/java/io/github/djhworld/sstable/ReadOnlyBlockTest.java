package io.github.djhworld.sstable;

import org.junit.Test;

import java.io.ByteArrayOutputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ReadOnlyBlockTest {

    @Test
    public void shouldFillEntireBlockWithOneEntry() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WriteableBlock writeableBlock = new WriteableBlock(16);
        writeableBlock.put("12345678");
        writeableBlock.flushTo(baos);

        ReadOnlyBlock readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        assertThat(readOnlyBlock.read(0), is("12345678"));
    }


    @Test
    public void shouldReadAllEntriesFromFullBlock() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WriteableBlock writeableBlock = new WriteableBlock(16);
        writeableBlock.put("1234");
        writeableBlock.put("5678");
        writeableBlock.flushTo(baos);


        ReadOnlyBlock readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        assertThat(readOnlyBlock.read(0), is("1234"));
        assertThat(readOnlyBlock.read(8), is("5678"));
    }

    @Test
    public void shouldReadAllEntriesFromBlockThatIsNotFull() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WriteableBlock writeableBlock = new WriteableBlock(16);
        writeableBlock.put("1234");
        writeableBlock.flushTo(baos);


        ReadOnlyBlock readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        assertThat(readOnlyBlock.read(0), is("1234"));
    }

    @Test
    public void shouldReadAllEntriesFromBlockThatIsNearlyFull() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WriteableBlock writeableBlock = new WriteableBlock(16);
        writeableBlock.put("123");
        writeableBlock.put("12");
        writeableBlock.flushTo(baos);


        ReadOnlyBlock readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        assertThat(readOnlyBlock.read(0), is("123"));
        assertThat(readOnlyBlock.read(7), is("12"));

        baos = new ByteArrayOutputStream();
        writeableBlock = new WriteableBlock(16);
        writeableBlock.put("1234");
        writeableBlock.put("123");
        writeableBlock.flushTo(baos);


        readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        assertThat(readOnlyBlock.read(0), is("1234"));
        assertThat(readOnlyBlock.read(8), is("123"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRaiseErrorForInvalidOffset() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        WriteableBlock writeableBlock = new WriteableBlock(8);
        writeableBlock.put("1234");
        writeableBlock.flushTo(baos);


        ReadOnlyBlock readOnlyBlock = new ReadOnlyBlock(baos.toByteArray());
        readOnlyBlock.read(1);
    }
}