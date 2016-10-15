package io.github.djhworld.io;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RewindableByteArrayOutputStreamTest {
    @Test
    public void shouldProduceInputStream() throws Exception {
        RewindableByteArrayOutputStream cos = new RewindableByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(cos);

        dos.writeInt(1);
        dos.writeInt(2);
        dos.writeInt(3);

        InputStream inputStream = cos.toInputStream();
        try (DataInputStream dis = new DataInputStream(inputStream)) {
            assertThat(dis.readInt(), is(1));
            assertThat(dis.readInt(), is(2));
            assertThat(dis.readInt(), is(3));
        }
    }

    @Test
    public void shouldRewindProduceCorrectInputStream() throws Exception {
        RewindableByteArrayOutputStream cos = new RewindableByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(cos);

        dos.writeInt(0);
        dos.writeInt(0);
        dos.writeInt(0);
        dos.writeInt(4);
        dos.writeInt(5);
        dos.writeInt(6);

        cos.rewind();

        dos.writeInt(1);
        dos.writeInt(2);
        dos.writeInt(3);


        InputStream inputStream = cos.toInputStream();
        try (DataInputStream dis = new DataInputStream(inputStream)) {
            assertThat(dis.readInt(), is(1));
            assertThat(dis.readInt(), is(2));
            assertThat(dis.readInt(), is(3));
            assertThat(dis.readInt(), is(4));
            assertThat(dis.readInt(), is(5));
            assertThat(dis.readInt(), is(6));
        }
    }
}