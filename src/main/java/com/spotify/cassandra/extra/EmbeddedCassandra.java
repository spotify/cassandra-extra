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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Run a cassandra instance in-process.
 */
public class EmbeddedCassandra implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCassandra.class);
  private static final URL CONFIG_TEMPLATE =
      EmbeddedCassandra.class.getClassLoader().getResource("embedded-cassandra-template.yaml");
  private static final int CQL_RETRIES = 5;

  private final ExecutorService executorService = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("EmbeddedCassandra")
      .build()
  );

  private final CassandraDaemon cassandra;

  private final Path dataDir;
  private final int port;
  private final int storagePort;
  private final int nativeTransportPort;
  private final AtomicBoolean running = new AtomicBoolean(false);

  public EmbeddedCassandra() throws IOException {
    this.dataDir = Files.createTempDirectory("cassandra-embedded");
    this.port = findFreePort();
    this.storagePort = findFreePort();
    this.nativeTransportPort = findFreePort();
    LOG.info("Starting cassandra on port {}, nativeTransportPort {}, storagePort {}, in dir {}",
             port, nativeTransportPort, storagePort, dataDir);

    checkNotNull(CONFIG_TEMPLATE, "Cassandra config template is null");
    String baseFile = Resources.toString(CONFIG_TEMPLATE, Charset.defaultCharset());

    String newFile = baseFile.replace("$DIR$", dataDir.toFile().getPath());
    newFile = newFile.replace("$PORT$", Integer.toString(port));
    newFile = newFile.replace("$STORAGE_PORT$", Integer.toString(storagePort));
    newFile = newFile.replace("$NATIVE_TRANSPORT_PORT$", Integer.toString(nativeTransportPort));

    Path configFile = dataDir.resolve("cassandra.yaml");
    Files.write(configFile, ImmutableSet.of(newFile), StandardCharsets.UTF_8);
    LOG.info("Cassandra config file: " + configFile);

    System.setProperty("cassandra.config", "file:" + configFile.toString());
    System.setProperty("cassandra-foreground", "true");

    cassandra = new CassandraDaemon();
    LOG.info("Embedded cassandra set up");
  }

  /**
   * Starts the embedded cassandra instance.
   * @throws EmbeddedCassandraException if cassandra can't start up
   */
  public void start() {
    if (running.compareAndSet(false, true)) {
      LOG.info("Starting Embedded Cassandra");
      Future<Void> startupFuture = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          cassandra.activate();
          LOG.info("Embedded Cassandra started");
          return null;
        }
      });

      try {
        startupFuture.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new EmbeddedCassandraException("Can't start up cassandra", e);
      }
    }
  }

  /**
   * Run cql statements on this embedded cassandra instance.
   * @throws EmbeddedCassandraException if the cql statements can't be run
   */
  public void runCQL(final Session session, final String cql) {
    checkState(running.get(), "not running");
    for (String statement : Splitter.on(';').omitEmptyStrings().trimResults().split(cql)) {
      int retries = CQL_RETRIES;
      while (retries-- > 0) {
        try {
          session.execute(statement + ';');
          break;
        } catch (DriverException e) {
          if (retries > 0) {
            LOG.warn("Cql error ({}). {} retries left", e.getMessage(), retries);
          } else {
            LOG.error("Cql error", e);
            throw new EmbeddedCassandraException("Can't run cql statement: " + statement, e);
          }
        }
      }
    }
  }

  /**
   * Get the native transport port.
   */
  public int getNativeTransportPort() {
    return nativeTransportPort;
  }

  /**
   * Get the thrift port.
   */
  public int getThriftTransportPort() {
    return port;
  }

  /**
   * Get the contact point.
   */
  public String getContactPoint() {
    return "127.0.0.1";
  }

  /**
   * Get the data directory.
   */
  public Path getDataDir() {
    return dataDir;
  }

  /**
   * Stops the embedded cassandra instance.
   */
  public void stop() {
    if (running.compareAndSet(true, false)) {
      LOG.info("Stopping Embedded Cassandra");
      cassandra.deactivate();
      executorService.shutdown();
      LOG.info("Deleting embedded cassandra tmp dir");
      try {
        Files.walkFileTree(this.dataDir, new DeleteDirRecursiveFileVisitor());
      } catch (IOException e) {
        LOG.warn("Can't clean up embedded cassandra tmp dir", e);
      }
      LOG.info("Embedded Cassandra stopped");
    }
  }

  @Override
  public void close() {
    stop();
  }

  private static int findFreePort() {
    try {
      final ServerSocket server = new ServerSocket(0);
      final int port = server.getLocalPort();
      server.close();
      return port;
    } catch (IOException e) {
      return -1;
    }
  }

  /** Delete a file tree recursively */
  private static final class DeleteDirRecursiveFileVisitor extends SimpleFileVisitor<Path> {

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
        throws IOException {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      if (exc != null) {
        return super.postVisitDirectory(dir, exc);
      }

      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }

}
