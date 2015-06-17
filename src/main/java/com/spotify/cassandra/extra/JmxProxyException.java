package com.spotify.cassandra.extra;

public class JmxProxyException extends Exception {

  public JmxProxyException(String msg) {
    super(msg);
  }

  public JmxProxyException(String msg, Exception e) {
    super(msg, e);
  }

  public JmxProxyException(Exception e) {
    super(e);
  }

}
