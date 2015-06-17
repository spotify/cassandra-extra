package com.spotify.cassandra.extra;

import com.google.common.annotations.VisibleForTesting;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import org.apache.cassandra.service.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Properties;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JmxProxy {

  private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);

  private static final int JMX_PORT = 7199;
  private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";
  private static final String SS_OBJECT_NAME = "org.apache.cassandra.db:type=StorageService";

  private final JMXConnector jmxConnector;
  private final StorageServiceMBean ssProxy;

  private JmxProxy(JMXConnector jmxConnector, StorageServiceMBean ssProxy) {
    this.jmxConnector = jmxConnector;
    this.ssProxy = ssProxy;
  }

  /**
   * Connects to JMX to a remote host, which must have enabled receiving connections
   * @throws JmxProxyException
   */
  public static JmxProxy connect(String host) throws JmxProxyException {
    assert null != host : "null host given to JmxProxy.connect()";
    String[] parts = host.split(":");
    if (parts.length == 2) {
      return connect(parts[0], Integer.valueOf(parts[1]));
    } else {
      return connect(host, JMX_PORT);
    }
  }

  /**
   * Connects to JMX to a remote host, which must have enabled receiving connections
   * @throws JmxProxyException
   */
  public static JmxProxy connect(String host, int port) throws JmxProxyException {
    try {
      JMXServiceURL jmxUrl = new JMXServiceURL(String.format(JMX_URL, host, port));
      return connect(jmxUrl);
    } catch (MalformedURLException e) {
      LOG.error(String.format("Failed to prepare the JMX connection to %s:%s", host, port));
      throw new JmxProxyException("Failure during preparations for JMX connection", e);
    }
  }

  /**
   * Connect to JMX of the current instance of jvm. Uses black magic.
   */
  @VisibleForTesting
  public static JmxProxy connectLocal() throws JmxProxyException {
    // figure out 'this' vm
    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    List<VirtualMachineDescriptor> vmds = VirtualMachine.list();
    VirtualMachineDescriptor vmd = null;
    for (VirtualMachineDescriptor v : vmds) {
      if (v.id().equals(pid)) {
        vmd = v;
        break;
      }
    }
    if (vmd == null) {
      throw new JmxProxyException("Could not find pid/vm I'm running in");
    }

    try {
      VirtualMachine vm = vmd.provider().attachVirtualMachine(vmd);
      Properties props = vm.getAgentProperties();
      String connectorAddress =
          props.getProperty("com.sun.management.jmxremote.localConnectorAddress");
      if (connectorAddress == null) {
        String agent = vm.getSystemProperties().getProperty(
            "java.home") + File.separator + "lib" + File.separator +
            "management-agent.jar";
        vm.loadAgent(agent);
        connectorAddress = vm.getAgentProperties().getProperty(
            "com.sun.management.jmxremote.localConnectorAddress");
      }
      JMXServiceURL url = new JMXServiceURL(connectorAddress);
      return connect(url);
    } catch (Exception e) {
      throw new JmxProxyException(e);
    }
  }

  private static JmxProxy connect(JMXServiceURL jmxUrl) throws JmxProxyException {
    try {
      ObjectName ssMbeanName = new ObjectName(SS_OBJECT_NAME);
      JMXConnector jmxConn = JMXConnectorFactory.connect(jmxUrl);
      MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();
      StorageServiceMBean ssProxy =
          JMX.newMBeanProxy(mbeanServerConn, ssMbeanName, StorageServiceMBean.class);
      return new JmxProxy(jmxConn, ssProxy);
    } catch (MalformedObjectNameException | IOException e) {
      throw new JmxProxyException(e);
    }
  }

  public void close() {
    try {
      this.jmxConnector.close();
    } catch (IOException e) {
      LOG.warn("Failed to close JMX connection: " + e.getMessage());
    }
  }

  public void createSnapshot(String keyspace, String table, String name) throws JmxProxyException {
    try {
      ssProxy.takeColumnFamilySnapshot(keyspace, table, name);
    } catch (IOException e) {
      LOG.error("Failed creating snapshot: " + e.getMessage());
      throw new JmxProxyException(e);
    }
  }

  public void clearSnapshot(String keyspace, String name) throws JmxProxyException {
    try {
      this.ssProxy.clearSnapshot(name, keyspace);
    } catch (IOException e) {
      LOG.error("Failed to clear snapshot: " + e.getMessage());
      throw new JmxProxyException(e);
    }
  }

  public void loadSStables(String ks, String table) {
    try {
       ssProxy.disableAutoCompaction(ks, table);
    } catch (IOException e) {
      e.printStackTrace();
    }
    ssProxy.loadNewSSTables(ks, table);
  }

}
