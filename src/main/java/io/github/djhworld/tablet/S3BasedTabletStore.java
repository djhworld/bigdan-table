package io.github.djhworld.tablet;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.github.djhworld.io.S3Sink;
import io.github.djhworld.io.S3Source;
import io.github.djhworld.io.Sink;
import io.github.djhworld.io.Source;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class S3BasedTabletStore extends TabletStore {
    private static final String S3 = "s3://";
    private final AmazonS3 amazonS3;

    public S3BasedTabletStore(AmazonS3 amazonS3, Path root) {
        super(root);
        this.amazonS3 = amazonS3;
    }

    @Override
    public List<Path> list(Integer currentGeneration) {
        List<S3ObjectSummary> summaries = listObjectsUnder(S3 + getCurrentGenerationPath(currentGeneration).toString() + "/");

        return summaries.stream()
                .filter(s3ObjectSummary -> !s3ObjectSummary.getKey().endsWith("/"))
                .map(s3ObjectSummary -> Paths.get(s3ObjectSummary.getBucketName(), s3ObjectSummary.getKey()).getFileName())
                .sorted()
                .collect(toList());
    }

    @Override
    public Source get(Integer currentGeneration, Path subLocation) {
        return new S3Source(amazonS3, getAmazonS3URI(currentGeneration, subLocation));
    }

    @Override
    public Sink newSink(Integer currentGeneration, Path subLocation) {
        return new S3Sink(amazonS3, getAmazonS3URI(currentGeneration, subLocation));
    }

    private List<S3ObjectSummary> listObjectsUnder(String s3url) {
        AmazonS3URI location = new AmazonS3URI(s3url);

        List<S3ObjectSummary> summaries = new ArrayList<>();
        ObjectListing objectListing = amazonS3.listObjects(location.getBucket(), location.getKey());
        summaries.addAll(objectListing.getObjectSummaries());
        while (objectListing.isTruncated()) {
            objectListing = amazonS3.listNextBatchOfObjects(objectListing);
            summaries.addAll(objectListing.getObjectSummaries());
        }
        return summaries;
    }

    private AmazonS3URI getAmazonS3URI(Integer currentGeneration, Path subLocation) {
        return new AmazonS3URI(S3 + getCurrentGenerationPath(currentGeneration).resolve(subLocation).toString());
    }
}
