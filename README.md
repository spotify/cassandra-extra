# cassandra-extra

Set of tools enhancing the experience with Apache Cassandra. For now, the project includes only EmbeddedCassandra and CassandraRule.

## Instalation

The instalation is done by pulling an artifact from Maven central:

````xml
<dependency>
    <groupId>com.spotify</groupId>
    <artifactId>cassandra-extra</artifactId>
    <version>0.0.1</version>
</dependency>
````

## EmbeddedCassandra

EmbeddedCassandra allows spawning a 1-node Apache Cassandra cluster within the currently running JVM. This is useful for some tests that go beyond simple client requests. For example, it's very useful to test things like integration with data processing technologies.

EmbeddedCassandra's API is intentionaly rather raw and feature-less. Please, see CassandraRule for more convenience.

## CassandraRule

CassandraRule wraps EmbeddedCassandra and exposes it as a JUnit @Rule. An example usage:

````java
  @Rule
  public CassandraRule cassandraRule = CassandraRule.newBuilder()
      .withManagedKeyspace()
      .withManagedTable("CREATE TABLE user (id TEXT PRIMARY KEY, email TEXT);")
      .build();

  Insert insertQuery = QueryBuilder
      .insertInto("user")
      .value("id", "zvo")
      .value("email", "foo@bar.baz");

  Select.Where selectQuery = QueryBuilder
      .select("id", "email")
      .from("user")
      .where(eq("id", "zvo"));

  @Test
  public void insertAndSelect() {
    Session session = cassandraRule.getSession();
    session.execute(insertQuery);
    List<Row> rows = session.execute(selectQuery).all();
    assertEquals(1, rows.size());
    assertEquals("zvo", rows.get(0).getString("id"));
    assertEquals("foo@bar.baz", rows.get(0).getString("email"));
  }
````

In the above example, `.withManagedKeyspace()` will create a randomly named keyspace and execute an `USE` command. Next, `.withManagedTable()` will create a table in the keyspace. Consequently, `.getSession()` will return a session with the keyspace already created and initialised. Keyspaces and tables are created and destroyed during `@Before`, resp. `@After` each `@Test`. Having the keyspace and table managed is optional; if this feature is not enabled, `@Before` and `@After` don't manipulate them.

# License

This software is released under the Apache License 2.0. More information in the file LICENSE distributed with this project.
