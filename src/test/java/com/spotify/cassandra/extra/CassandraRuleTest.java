package com.spotify.cassandra.extra;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Resources;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import org.junit.Test;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CassandraRuleTest {

  private static final URL TABLE_SCHEMA = Resources.getResource("cf.cql");
  private static final String KEY = "key";
  private static final ByteBuffer VALUE = ByteBuffer.wrap(new byte[] {1, 2});

  @Test
  public void managed() throws Throwable {
    final CassandraRule rule = CassandraRule.newBuilder()
        .withManagedKeyspace()
        .withManagedTable(TABLE_SCHEMA)
        .build();
    rule.before();

    final Session session = rule.getSession();

    Insert insert = QueryBuilder.insertInto("mytable")
        .value("key", KEY)
        .value("value", VALUE);
    session.execute(insert);

    Statement select = QueryBuilder.select()
        .from("mytable")
        .where(QueryBuilder.eq("key", KEY))
        .limit(1);
    Row result = session.execute(select).all().get(0);

    assertEquals(KEY, result.getString("key"));
    assertEquals(VALUE, result.getBytes("value"));

    rule.after();
  }

  @Test
  public void unmanaged() throws Throwable {
    final CassandraRule rule = CassandraRule.newBuilder().build();
    rule.before();
    final Session session = rule.getSession();

    session.execute("CREATE KEYSPACE unmanaged WITH replication = " +
                    "{'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
    session.execute("CREATE TABLE unmanaged.unmanaged (foo TEXT PRIMARY KEY, bar INT );");
    session.execute("INSERT INTO unmanaged.unmanaged (foo, bar) VALUES ('baz', 42);");
    session.execute("USE unmanaged;");

    Statement select = QueryBuilder.select().from("unmanaged");
    List<Row> rows = session.execute(select).all();
    assertEquals(1, rows.size());
    assertEquals("baz", rows.get(0).getString("foo"));
    assertEquals(42, rows.get(0).getInt("bar"));

    session.execute("DROP KEYSPACE unmanaged;");

    rule.after();
  }

  @Test
  public void delegatesToSupplier() {
    final int nativePort = 5678;
    final EmbeddedCassandra cassandra = mock(EmbeddedCassandra.class);
    when(cassandra.getNativeTransportPort()).thenReturn(nativePort);

    final Supplier<EmbeddedCassandra> supplier = Suppliers.ofInstance(cassandra);

    final CassandraRule rule = new CassandraRule(supplier, false, false, null);
    rule.getSession();

    assertEquals(nativePort, rule.getNativeTransportPort());
  }

}
