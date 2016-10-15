package io.github.djhworld.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static java.nio.file.Paths.get;

public class S3Source implements Source {
    private final AmazonS3 amazonS3;
    private final AmazonS3URI s3Key;

    public S3Source(AmazonS3 amazonS3, AmazonS3URI s3Key) {
        this.amazonS3 = amazonS3;
        this.s3Key = s3Key;
    }

    @Override
    public InputStream open() {
        S3Object object = amazonS3.getObject(
                s3Key.getBucket(),
                s3Key.getKey()
        );
        return object.getObjectContent();
    }

    @Override
    public InputStream getRange(int offset, int length) throws IOException {
        S3Object object = amazonS3.getObject(
                new GetObjectRequest(s3Key.getBucket(), s3Key.getKey())
                        .withRange(offset, offset + length)
        );
        return object.getObjectContent();
    }

    @Override
    public Path getLocation() {
        return get(s3Key.getURI().getRawPath());
    }
}
