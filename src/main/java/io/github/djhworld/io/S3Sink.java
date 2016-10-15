package io.github.djhworld.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class S3Sink implements Sink {
    private final AmazonS3 amazonS3;
    private final AmazonS3URI s3Key;

    public S3Sink(AmazonS3 amazonS3, AmazonS3URI s3Key) {
        this.amazonS3 = amazonS3;
        this.s3Key = s3Key;
    }

    @Override
    public void flush(ByteBuffer buffer) throws IOException {
        int contentLength = buffer.limit();

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(contentLength);

        amazonS3.putObject(
                new PutObjectRequest(
                        s3Key.getBucket(),
                        s3Key.getKey(),
                        new ByteBufferBackedInputStream(buffer),
                        objectMetadata)
        );
    }
}
