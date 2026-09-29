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
package org.apache.solr.cloud;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.core.CoreContainer;

/**
 * What makes a MiniSolrCloudCluster a GraphChecker workload: its nodes
 * registered before the window opens, and the world the window starts from
 * the same in the recording and in every replay.
 *
 * <p>With {@link #AUTOMATIC} set, any cluster test is a workload as it is
 * written.  Each cluster registers its nodes as it comes up ({@code
 * auto<n>/node<i>}, in the order of their names) and its client; the first
 * one opens the window with one request on the thread that built it, and the
 * test method runs under a request of its own ({@link #beginTest}), since
 * the randomized runner gives a test a thread of its own.  The window closes
 * with the test ({@link #endTest}): the cluster's teardown is not recorded.</p>
 *
 * <p>The same world at the window: every node the cluster starts comes up in
 * turn, so the first one is the Overseer, on a port above the one before it,
 * so the nodes' names -- which placement orders -- sort the way they were
 * started; and nothing the cluster sets up expires on the wall clock (ZK
 * sessions, socket and update timeouts, the test's own waits for a state on
 * its client), because the recording runs on a JIT and every replay on an
 * interpreter many times slower.</p>
 */
public final class CausynthSolrCluster {
  /** Set on a workload JVM whose cluster tests were written without GraphChecker. */
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  private static final String NODE = "CLUSTER_NODE";
  /** Longer than any case runs: an expiry that cannot fire. */
  public static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);
  /** The system properties the cluster's solr.xml reads its timeouts from. */
  private static final String[] TIMEOUTS = {"solr.zkclienttimeout", "socketTimeout",
      "connTimeout", "distribUpdateConnTimeout", "distribUpdateSoTimeout"};

  private static final Map<MiniSolrCloudCluster, String> CLUSTERS = new IdentityHashMap<>();
  private static final Map<JettySolrRunner, String> NODES = new IdentityHashMap<>();
  private static MiniSolrCloudCluster windowCluster;
  private static long suiteRequest;
  private static long testRequest;
  private static int lastPort;

  private CausynthSolrCluster() {
  }

  public static boolean automatic() {
    return Boolean.getBoolean(AUTOMATIC);
  }

  /** Before the cluster starts: its timeouts, and its ZooKeeper's session bound. */
  public static void configure(ZkTestServer zkServer) {
    for (String timeout : TIMEOUTS) {
      System.setProperty(timeout, Integer.toString(NEVER_MS));
    }
    if (zkServer != null) {
      zkServer.setMaxSessionTimeout(NEVER_MS);
    }
    // How long a collections API call waits for the Overseer to answer.
    org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT = NEVER_MS;
  }

  /**
   * The next node's port: free, and above every port handed out before, so
   * the nodes' names sort in the order they started.
   */
  public static synchronized int nextPort() throws IOException {
    if (lastPort == 0) {
      // A random start: replays of one case share a host.
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
  }

  /**
   * Registers a cluster's nodes and client as it comes up, or the nodes it
   * started since; the first cluster opens the window.
   */
  public static synchronized void registerAutomatically(MiniSolrCloudCluster cluster) {
    String prefix = CLUSTERS.get(cluster);
    if (prefix == null) {
      prefix = "auto" + CLUSTERS.size();
      CLUSTERS.put(cluster, prefix);
    }
    registerStartedNodes(cluster);
    CloudSolrClient client = cluster.getSolrClient();
    if (windowCluster == null && client != null) {
      windowCluster = cluster;
      MessageDoors.registerNode(client, "EXTERNAL_APP", "SOLR_CLIENT", prefix + "/client0",
          client.getLbClient(), client.getZkStateReader(),
          client.getZkStateReader().getZkClient());
      client.getZkStateReader().causynthUnboundedWaits();
      MessageDoors.startRecording();
      suiteRequest = MessageDoors.beginRequest(client, "suite");
    }
  }

  /** The nodes a registered cluster started since, named in the order of their names. */
  public static synchronized void registerStartedNodes(MiniSolrCloudCluster cluster) {
    String prefix = CLUSTERS.get(cluster);
    if (prefix == null) {
      return;
    }
    List<JettySolrRunner> started = new ArrayList<>(cluster.getJettySolrRunners());
    started.sort(Comparator.comparing(JettySolrRunner::getSolrHome));
    for (JettySolrRunner jetty : started) {
      if (NODES.containsKey(jetty) || jetty.getCoreContainer() == null) {
        continue;
      }
      String name = prefix + "/node" + NODES.size();
      NODES.put(jetty, name);
      CoreContainer cores = jetty.getCoreContainer();
      ZkController zk = cores.getZkController();
      MessageDoors.registerNode(jetty, NODE, "SOLR_NODE", name,
          cores, jetty.getSolrDispatchFilter(), cores.getUpdateShardHandler(),
          cores.getShardHandlerFactory(), zk,
          zk == null ? null : zk.getZkClient(),
          zk == null ? null : zk.getZkStateReader(),
          zk == null ? null : zk.getOverseer());
    }
  }

  /** The test method's own request, on the thread the runner gives it. */
  public static synchronized void beginTest() {
    if (windowCluster != null && testRequest == 0L) {
      testRequest = MessageDoors.beginRequest(windowCluster.getSolrClient(), "test");
    }
  }

  /** The test is over: its request ends, and so does the window. */
  public static synchronized void endTest() {
    if (testRequest != 0L) {
      MessageDoors.endRequest(testRequest, "test");
      testRequest = 0L;
      MessageDoors.stopRecording();
    }
  }

  /** The window's suite request ends when its cluster shuts down. */
  public static synchronized void shutdownAutomatically(MiniSolrCloudCluster cluster) {
    if (cluster == windowCluster) {
      MessageDoors.endRequest(suiteRequest, "suite");
      windowCluster = null;
    }
  }
}
