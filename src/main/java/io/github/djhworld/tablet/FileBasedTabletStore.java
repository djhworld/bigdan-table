package io.github.djhworld.tablet;

import io.github.djhworld.io.FileSink;
import io.github.djhworld.io.FileSource;
import io.github.djhworld.io.Sink;
import io.github.djhworld.io.Source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static java.nio.file.Files.createDirectories;
import static java.util.stream.Collectors.toList;

public class FileBasedTabletStore extends TabletStore {
    public FileBasedTabletStore(Path root) throws IOException {
        super(root);
        createIfNotExists();
    }

    @Override
    public List<Path> list(Integer currentGeneration) throws IOException {
        Path currentGenerationPath = getCurrentGenerationPath(currentGeneration);
        createDirectories(currentGenerationPath);

        return Files.list(currentGenerationPath)
                .sorted()
                .map(Path::getFileName)
                .collect(toList());
    }

    @Override
    public Source get(Integer currentGeneration, Path subLocation) throws IOException {
        Path currentGenerationPath = getCurrentGenerationPath(currentGeneration);
        createDirectories(currentGenerationPath);

        return new FileSource(getCurrentGenerationPath(currentGeneration).resolve(subLocation));
    }

    @Override
    public Sink newSink(Integer currentGeneration, Path subLocation) throws IOException {
        Path currentGenerationPath = getCurrentGenerationPath(currentGeneration);
        createDirectories(currentGenerationPath);

        return new FileSink(currentGenerationPath.resolve(subLocation));
    }

    private void createIfNotExists() throws IOException {
        createDirectories(getRoot());
    }
}
