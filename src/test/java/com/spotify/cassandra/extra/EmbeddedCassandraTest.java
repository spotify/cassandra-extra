package com.spotify.cassandra.extra;

import org.apache.cassandra.service.CassandraDaemon;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class EmbeddedCassandraTest {

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private Path dataDir;

  @Before
  public void setUp() throws IOException {
    dataDir = Paths.get(tmp.newFolder().toURI());
  }

  @Test
  public void startOnce() throws IOException {
    CassandraDaemon daemon = mock(CassandraDaemon.class);
    EmbeddedCassandra cass = new EmbeddedCassandra(dataDir, 1, 2, 3, daemon);

    cass.start();
    cass.start();
    verify(daemon, times(1)).activate();
  }

  @Test
  public void clearsDataDirectory() throws IOException {
    CassandraDaemon daemon = mock(CassandraDaemon.class);
    EmbeddedCassandra cass = new EmbeddedCassandra(dataDir, 1, 2, 3, daemon);
    cass.start();

    // create a file in dataDir
    Path somefile = dataDir.resolve("somefile");
    Files.write(somefile, "hello world".getBytes());
    assertTrue(Files.exists(somefile));

    cass.stop();
    assertFalse(Files.exists(somefile));
  }

  @Test
  public void createsConfig() throws IOException {
    CassandraDaemon daemon = mock(CassandraDaemon.class);
    EmbeddedCassandra cass = new EmbeddedCassandra(dataDir, 1, 2, 3, daemon);
    cass.start();

    Path configFile = dataDir.resolve("cassandra.yaml");
    assertTrue(Files.exists(configFile));

    String expectedPath = configFile.toAbsolutePath().toString();
    assertThat(System.getProperty("cassandra.config"), equalTo("file:" + expectedPath));
  }

  @Test
  public void differentPorts() throws IOException {
    EmbeddedCassandra c1 = EmbeddedCassandra.create();
    EmbeddedCassandra c2 = EmbeddedCassandra.create();

    assertThat(c1.getNativeTransportPort(), not(equalTo(c2.getNativeTransportPort())));
    assertThat(c1.getThriftTransportPort(), not(equalTo(c2.getThriftTransportPort())));
  }

}
