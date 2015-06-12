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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CassandraRuleTestUnmanaged {

  String KEYSPACE_SCHEMA = "CREATE KEYSPACE unmanaged WITH replication = " +
      "{'class' : 'SimpleStrategy', 'replication_factor' : 1 }";
  String TABLE_SCHEMA = "CREATE TABLE unmanaged.unmanaged (foo TEXT PRIMARY KEY, bar INT );";
  String INSERT_STATEMENT = "INSERT INTO unmanaged.unmanaged (foo, bar) VALUES ('baz', 42);";
  String USE_STATEMENT = "USE unmanaged;";

  @Rule
  public CassandraRule cassandraRule = CassandraRule.newBuilder().build();

  @Test
  public void queryRoundtrip() throws Exception {
    Session session = cassandraRule.getSession();

    session.execute(KEYSPACE_SCHEMA);
    session.execute(TABLE_SCHEMA);
    session.execute(INSERT_STATEMENT);
    session.execute(USE_STATEMENT);

    Statement select = QueryBuilder.select().from("unmanaged");
    List<Row> rows = session.execute(select).all();
    assertEquals(1, rows.size());
    assertEquals("baz", rows.get(0).getString("foo"));
    assertEquals(42, rows.get(0).getInt("bar"));
  }

}
