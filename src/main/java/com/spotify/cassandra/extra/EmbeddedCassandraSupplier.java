package com.spotify.cassandra.extra;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provide access to an {@link com.spotify.cassandra.extra.EmbeddedCassandra} instance.
 *
 * Since Cassandra can only be run once per JVM, only one instance will be created per JVM on
 * the first call to {@link com.spotify.cassandra.extra.EmbeddedCassandraSupplier#get}. Subsequent
 * access from any Supplier will yield the same instance.
 */
class EmbeddedCassandraSupplier implements Supplier<EmbeddedCassandra> {

  private static final AtomicReference<EmbeddedCassandra> CASSANDRA = new AtomicReference<>();

  @Override
  public EmbeddedCassandra get() {
    EmbeddedCassandra instance = CASSANDRA.get();
    if (instance != null) {
      return instance;
    }

    try {
      instance = new EmbeddedCassandra();
      if (CASSANDRA.compareAndSet(null, instance)) {
        instance.start();
        return instance;
      }
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
    return CASSANDRA.get();
  }

}
