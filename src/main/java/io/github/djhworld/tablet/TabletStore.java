package io.github.djhworld.tablet;

import io.github.djhworld.io.Sink;
import io.github.djhworld.io.Source;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public abstract class TabletStore {
    private final Path root;

    TabletStore(Path root) {
        this.root = root;
    }

    protected final Path getRoot() {
        return root;
    }

    protected final Path getCurrentGenerationPath(Integer currentGeneration) {
        return getRoot().resolve(Paths.get(currentGeneration.toString()));
    }

    public abstract List<Path> list(Integer currentGeneration) throws IOException;

    public abstract Source get(Integer currentGeneration, Path subLocation) throws IOException;

    public abstract Sink newSink(Integer currentGeneration, Path subLocation) throws IOException;
}
