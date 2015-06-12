package com.spotify.cassandra.extra;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provide access to an {@link com.spotify.cassandra.extra.EmbeddedCassandra} instance.
 *
 * Since cassandra can only be run once per JVM, only one instance will be created per JVM on
 * the first call to {@link com.spotify.cassandra.extra.EmbeddedCassandraSupplier#get} and cached
 * for subsequent access.
 */
class EmbeddedCassandraSupplier implements Supplier<EmbeddedCassandra> {

  private static AtomicReference<EmbeddedCassandra> embeddedCassandra = new AtomicReference<>();

  @Override
  public EmbeddedCassandra get() {
    ensureCassandraStarted();
    return embeddedCassandra.get();
  }

  private void ensureCassandraStarted() {
    try {
      EmbeddedCassandra newEmbeddedCassandra = new EmbeddedCassandra();
      if (embeddedCassandra.compareAndSet(null, newEmbeddedCassandra)) {
        newEmbeddedCassandra.start();
      }
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

}
