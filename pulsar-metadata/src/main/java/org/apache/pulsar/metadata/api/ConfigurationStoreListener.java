package org.apache.pulsar.metadata.api;

import java.util.Map;

public interface ConfigurationStoreListener<T> {
    public void onUpdate(String path, T data, Map value);
}