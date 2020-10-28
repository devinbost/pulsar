package org.apache.pulsar.metadata.api;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface TypedMetadataCache<T> {
    // ...

    /**
     * Perform an atomic read-modify-update of the value.
     *
     * The modify function can potentially be called multiple times if there are concurrent updates happening.
     *
     * @param path
     *            the path of the value
     * @param modifyFunction
     *            a function that will be passed the current value and returns a modified value to be stored
     * @return a future to track the completion of the operation
     */
    CompletableFuture<Void> readModifyUpdate(String path, Function<T, T> modifyFunction);
}
