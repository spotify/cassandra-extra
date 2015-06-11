package com.spotify.cassandra.extra;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EmbeddedCassandraSupplierTest {

  @Test
  public void cachesInstance() {
    EmbeddedCassandraSupplier supplier = new EmbeddedCassandraSupplier();
    EmbeddedCassandra c1 = supplier.get();
    EmbeddedCassandra c2 = supplier.get();
    assertEquals(c1, c2);
  }

  @Test
  public void cachesInstanceAcrossSuppliers() {
    EmbeddedCassandra c1 = new EmbeddedCassandraSupplier().get();
    EmbeddedCassandra c2 = new EmbeddedCassandraSupplier().get();
    assertEquals(c1, c2);
  }

}
