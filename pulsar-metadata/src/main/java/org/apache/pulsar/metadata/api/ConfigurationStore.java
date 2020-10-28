package org.apache.pulsar.metadata.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ConfigurationStore {
    CompletableFuture<List<String>> getTenants();

    CompletableFuture<List<String>> getNamespaces(String tenant);
}
