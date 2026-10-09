/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;

/**
 * The ports an automatic cluster's servers listen on ({@link
 * CausynthBounds#AUTOMATIC}): free, and each above every one handed out
 * before, so the nodes' names -- host and port, which live-node lists sort
 * and placement and clients' shuffles order -- sort the way the servers
 * were made, in the recording and in every replay.  A Jetty given no port,
 * and a socket proxy in front of one, take theirs here
 * (JettySolrRunner).  An AbstractFullDistribZkTestBase test's servers,
 * on random ports, sorted otherwise in each run, and a seeded shuffle of
 * them chose another node (solr-adhoc-31, round MB18s).
 */
public final class CausynthPorts {
  private static int lastPort;

  private CausynthPorts() {
  }

  /** The next port: the first at random (replays of one case share a host), then increasing. */
  public static synchronized int next() {
    try {
      if (lastPort == 0) {
        try (ServerSocket socket = new ServerSocket(0)) {
          lastPort = socket.getLocalPort();
          return lastPort;
        }
      }
      for (int port = lastPort + 1; port < 65536; port++) {
        try (ServerSocket socket = new ServerSocket(port)) {
          socket.setReuseAddress(true);
          lastPort = port;
          return port;
        } catch (IOException taken) {
          // the next one
        }
      }
      throw new IOException("no free port above " + lastPort);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
