package org.apache.pulsar.metadata.api;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class TypedMetadataCacheImpl<T> implements TypedMetadataCache<T> {

    @Override
    public CompletableFuture<Void> readModifyUpdate(String path, Function<T, T> modifyFunction) {
        return null;
    }
}
