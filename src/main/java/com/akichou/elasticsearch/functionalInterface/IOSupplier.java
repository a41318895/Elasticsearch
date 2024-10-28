package com.akichou.elasticsearch.functionalInterface;

import java.io.IOException;

@FunctionalInterface
public interface IOSupplier<V> {

    V get() throws IOException ;
}
