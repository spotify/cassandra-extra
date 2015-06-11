/*
 * Copyright (c) 2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.cassandra.extra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closeables;
import com.google.common.io.Resources;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Creates a 1-node Cassandra cluster for testing. Can be used as @Rule, or independently.
 *
 * If desired, can automatically manage a keyspace. This translates to creating a randomly-named
 * keyspace during @Before and dropping it during @After.
 *
 * If a table schema is provided, a table will be managed also. Table schema can be provided
 * as a String or as a path to something resolvable by {@link com.google.common.io.Resources}.
 * Providing a table schema causes this rule to create the table during @Before. The table is
 * deleted during @After when the keyspace is dropped. Having the table managed requires having
 * the keyspace managed as well.
 *
 */
public class CassandraRule extends ExternalResource {

  private static final Logger logger = Logger.getLogger(CassandraRule.class.getName());

  public static final String CREATE_KEYSPACE_QUERY = "CREATE KEYSPACE %s "
      + "WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1 } "
      + "AND DURABLE_WRITES = true;";
  public static final String USE_KEYSPACE_QUERY = "USE %s;";
  public static final String DROP_KEYSPACE_QUERY = "DROP KEYSPACE %s;";

  private static AtomicReference<EmbeddedCassandra> embeddedCassandra = new AtomicReference<>();

  private final boolean manageKeyspace;
  private final boolean manageTable;
  private final String keyspaceName;
  private final String tableSchema;

  private Cluster cluster;
  private Session session;

  private CassandraRule(boolean manageKeyspace, boolean manageTable, String actualTableSchema) {
    this.manageKeyspace = manageKeyspace;
    this.keyspaceName = "cqlunit_" + UUID.randomUUID().toString().replace("-", "");
    this.manageTable = manageTable;
    this.tableSchema = actualTableSchema;
  }

  public Session getSession() {
    return session;
  }

  public String getKeyspaceName() {
    return keyspaceName;
  }

  public Cluster getCluster() {
    final Cluster cluster = mock(Cluster.class);
    when(cluster.connect(anyString())).thenReturn(session);
    return cluster;
  }

  /**
   * Note: Preferred usage is to connect via {@link CassandraRule#getCluster()}
   */
  public int getNativeTransportPort() {
    return embeddedCassandra.get().getNativeTransportPort();
  }

  /**
   * Note: Preferred usage is to connect via {@link CassandraRule#getCluster()}
   */
  public int getThriftPort() {
    return embeddedCassandra.get().getThriftTransportPort();
  }

  public Path getDataDir() {
    return embeddedCassandra.get().getDataDir();
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    ensureCassandraStarted();

    cluster = Cluster.builder()
        .withPort(embeddedCassandra.get().getNativeTransportPort())
        .addContactPoints(embeddedCassandra.get().getContactPoint())
        .build();

    session = cluster.connect();

    if (manageKeyspace) {
      String createQuery = String.format(CREATE_KEYSPACE_QUERY, keyspaceName);
      session.execute(createQuery);
      String useQuery = String.format(USE_KEYSPACE_QUERY, keyspaceName);
      session.execute(useQuery);
    }

    if (manageTable) {
      session.execute(tableSchema);
    }
  }

  private void ensureCassandraStarted() throws IOException {
    EmbeddedCassandra newEmbeddedCassandra = new EmbeddedCassandra();
    if (embeddedCassandra.compareAndSet(null, newEmbeddedCassandra)) {
      newEmbeddedCassandra.start();
    }
  }

  @VisibleForTesting
  protected void cleanup() {
    Session session = this.session.isClosed() ? cluster.connect() : this.session;
    if (manageKeyspace) {
      final String dropQuery = String.format(DROP_KEYSPACE_QUERY, keyspaceName);
      session.execute(dropQuery);
    }
  }

  @Override
  protected void after() {
    super.after();
    try {
      cleanup();
      Closeables.close(session, true);
    } catch (DriverException ex) {
      logger.log(Level.WARNING, "can't drop keyspace after test", ex);
    } catch (IOException ex) {
      logger.log(Level.WARNING, "can't close session", ex);
    } finally {
      try {
        Closeables.close(cluster, true);
      } catch (IOException ex) {
        logger.log(Level.WARNING, "can't close cluster", ex);
      }
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private boolean manageKeyspace = false;
    private URL tableSchemaFile = null;
    private String tableSchema = null;

    public Builder withManagedKeyspace() {
      this.manageKeyspace = true;
      return this;
    }

    public Builder withManagedTable(URL tableSchemaFile) {
      this.tableSchemaFile = tableSchemaFile;
      return this;
    }

    public Builder withManagedTable(String tableSchema) {
      this.tableSchema = tableSchema;
      return this;
    }

    public CassandraRule build() {
      boolean manageTable = false;
      String actualTableSchema = "";
      if (tableSchemaFile != null) {
        try {
          actualTableSchema = Resources.toString(tableSchemaFile, StandardCharsets.UTF_8);
          manageTable = true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else if (tableSchema != null) {
        actualTableSchema = tableSchema;
        manageTable = true;
      }
      if (!manageKeyspace && manageTable) {
        throw new RuntimeException("Can't help with managing a table if the keyspace is not " +
            "managed as well");
      }
      return new CassandraRule(manageKeyspace, manageTable, actualTableSchema);
    }

  }
}
