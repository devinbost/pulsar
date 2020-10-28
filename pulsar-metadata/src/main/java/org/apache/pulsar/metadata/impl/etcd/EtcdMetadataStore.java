package org.apache.pulsar.metadata.impl.etcd;


import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Stat;

import com.ibm.etcd;
import io.etcd.jetcd.Client;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EtcdMetadataStore implements MetadataStore {
    //private final ExecutorService executor;
    public EtcdMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig) throws IOException {
        Client client = Client.builder().endpoints("http://localhost:2379").build();
        KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379).withPlainText().build();
/*
        try {
            // Do stuff to setup etcd here.
            Client client = Client.builder().endpoints
            // Setup connection with connection string to Etcd

        } catch (InterruptedException e) {
            throw new IOException(e);
        }

        this.executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("etcd-metadata-store-callback"));
    */
    }
    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getChildren(String path) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> exists(String path) {
        return null;
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(String path, Optional<Long> expectedVersion) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
