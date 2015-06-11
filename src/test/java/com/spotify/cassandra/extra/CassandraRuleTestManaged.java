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
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.io.Resources;
import org.junit.Rule;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class CassandraRuleTestManaged extends CassandraRuleTest {

  @Rule
  public CassandraRule cassandraRule = CassandraRule.newBuilder()
      .withManagedKeyspace()
      .withManagedTable(Resources.getResource("cf.cql"))
      .build();

  @Override
  public CassandraRule getCassandraRule() {
    return cassandraRule;
  }

  @Override
  public void queryRoundtrip() throws Exception {
    ClusterConnection clusterConnection = getCassandraRule().getClusterConnection();
    Session session = clusterConnection.getCluster().connect("mytable");

    final String key = "mykey";
    final ByteBuffer value = ByteBuffer.wrap(new byte[] {1, 2});
    Insert insert = QueryBuilder.insertInto("mytable")
        .value("key", key)
        .value("value", value);
    session.execute(insert);

    Statement select = QueryBuilder.select()
        .from("mytable")
        .where(QueryBuilder.eq("key", key))
        .limit(1);
    Row result = session.execute(select).all().get(0);

    assertEquals(key, result.getString("key"));
    assertEquals(value, result.getBytes("value"));
  }

}
