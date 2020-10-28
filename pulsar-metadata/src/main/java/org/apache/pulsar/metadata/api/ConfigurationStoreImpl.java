package org.apache.pulsar.metadata.api;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ConfigurationStoreImpl implements ConfigurationStore {
    private MetadataStore store;
    public ConfigurationStoreImpl(){
        MetadataStoreConfig config = MetadataStoreConfig.builder().build();
        try{
            store = MetadataStoreFactory.create(config.getMetastoreConnectionString(), config);
        } catch (IOException ex){
            System.out.println("Error in ConfigurationStore: " + ex.getMessage());
        }

    }

    @Override
    public CompletableFuture<List<String>> getTenants() {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getNamespaces(String tenant) {
        return null;
    }
}
