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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.BaseCloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.common.util.CausynthBounds;
import org.apache.solr.common.util.CausynthRandom;
import org.apache.solr.core.CoreContainer;

/**
 * What makes a MiniSolrCloudCluster a GraphChecker workload: its nodes
 * registered before the window opens, and the world the window starts from
 * the same in the recording and in every replay.
 *
 * <p>With {@link CausynthBounds#AUTOMATIC} set, any cluster test is a workload as it is
 * written.  Each cluster registers its nodes as it comes up ({@code
 * auto<n>/node<i>}, in the order of their names) and its client, and every
 * cloud client a test builds against it ({@link #registerClient}); the first
 * one opens the window with one request on the thread that built it, and the
 * test method runs under a request of its own ({@link #beginTest}), since
 * the randomized runner gives a test a thread of its own -- opened when the
 * window opens if the test built its cluster in its own {@code @Before}.  The
 * window closes with the test ({@link #endTest}) or with its cluster's
 * shutdown, whichever comes first: the teardown is not recorded.  A jetty
 * that starts in the window is a node from its first moment ({@link
 * Starts}): a registered one restarted at its next epoch, a new one of a
 * registered cluster the next node of that cluster.  A client the test builds
 * outside SolrTestCaseJ4's cloud builders -- an HttpSolrClient to a node --
 * is no node: what it sends on the test's thread is the thread's, its node
 * and its request.</p>
 *
 * <p>The same world at the window: every node the cluster starts comes up in
 * turn, so the first one is the Overseer, on a port above the one before it,
 * so the nodes' names -- which placement orders -- sort the way they were
 * started; and nothing the cluster sets up expires on the wall clock (ZK
 * sessions, socket and update timeouts, every wait for a cluster state),
 * because the recording runs on a JIT and every replay on an interpreter
 * many times slower.</p>
 */
public final class CausynthSolrCluster {
  private static final String NODE = "CLUSTER_NODE";
  /** The system properties the cluster's solr.xml reads its timeouts from. */
  private static final String[] TIMEOUTS = {"solr.zkclienttimeout", "socketTimeout",
      "connTimeout", "distribUpdateConnTimeout", "distribUpdateSoTimeout"};

  private static final Map<MiniSolrCloudCluster, String> CLUSTERS = new IdentityHashMap<>();
  /** The registered clusters' and servers' prefixes by the ZooKeeper address their nodes and clients name. */
  private static final Map<String, String> PREFIXES = new HashMap<>();
  private static final Map<JettySolrRunner, String> NODES = new IdentityHashMap<>();
  /** A registered node's restarts so far: its epoch. */
  private static final Map<JettySolrRunner, Long> EPOCHS = new IdentityHashMap<>();
  /** Per cluster prefix, the clients a test built against it. */
  private static final Map<String, Integer> CLIENTS = new HashMap<>();
  /** What opened the window: a cluster, or an AbstractFullDistribZkTestBase's servers. */
  private static Object windowOwner;
  private static CloudSolrClient windowClient;
  private static boolean testRunning;
  private static long suiteRequest;
  private static long testRequest;

  static {
    JettySolrRunner.causynthStarts = new Starts();
  }

  private CausynthSolrCluster() {
  }

  /** Before the cluster starts: its timeouts, and its ZooKeeper's session bound. */
  public static void configure(ZkTestServer zkServer) {
    for (String timeout : TIMEOUTS) {
      System.setProperty(timeout, Integer.toString(CausynthBounds.NEVER_MS));
    }
    if (zkServer != null) {
      zkServer.setMaxSessionTimeout(CausynthBounds.NEVER_MS);
    }
    // How long a collections API call waits for the Overseer to answer.
    org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT = CausynthBounds.NEVER_MS;
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
    registerNodes(prefix, cluster.getJettySolrRunners());
    CloudSolrClient client = cluster.getSolrClient();
    if (client != null) {
      PREFIXES.put(client.getZkHost(), prefix);
    }
    if (windowOwner == null && client != null) {
      openWindow(cluster, prefix, client);
    }
  }

  /**
   * The servers an AbstractFullDistribZkTestBase test built for itself (no
   * MiniSolrCloudCluster), once they are up: registered as a cluster's are
   * ({@code servers0}), and the window opened on the thread that built them,
   * the test's, with the test running.  The test's own timeouts stay: such a
   * test partitions nodes and waits on its bounds.
   */
  public static synchronized void registerServers(List<JettySolrRunner> jettys, CloudSolrClient client) {
    registerNodes("servers0", jettys);
    PREFIXES.put(client.getZkHost(), "servers0");
    if (windowOwner == null) {
      openWindow(jettys, "servers0", client);
      beginTest();
    }
  }

  /**
   * A cloud client a test built against a registered cluster or servers --
   * through SolrTestCaseJ4's builder, which may point it at the cluster's
   * ZooKeeper or at its nodes -- is a client node of that cluster ({@code
   * <cluster>/client<k>}, k in the order they were built): its requests and
   * its state reader's watchers are its own, not the test thread's.
   */
  public static synchronized void registerClient(MiniSolrCloudCluster built,
      BaseCloudSolrClient client, Object lbClient) {
    String prefix = built != null ? CLUSTERS.get(built)
        : client.getClusterStateProvider() instanceof ZkClientClusterStateProvider
            ? PREFIXES.get(client.getZkHost()) : null;
    if (prefix == null) {
      return;
    }
    int clients = CLIENTS.merge(prefix, 1, Integer::sum);
    registerClientNode(prefix + "/client" + clients, client, lbClient);
  }

  /** The test method's own request, on the thread the runner gives it, once
   *  the window is open: here, or when the test's cluster comes up in its
   *  own {@code @Before} ({@link #openWindow}). */
  public static synchronized void beginTest() {
    testRunning = true;
    if (windowOwner != null && testRequest == 0L) {
      testRequest = MessageDoors.beginRequest(windowClient, "test");
    }
  }

  /** The test is over: its request ends, and so does the window. */
  public static synchronized void endTest() {
    testRunning = false;
    if (testRequest != 0L) {
      MessageDoors.endRequest(testRequest, "test");
      testRequest = 0L;
      MessageDoors.stopRecording();
    }
  }

  /** The servers' test is over. */
  public static synchronized void shutdownServers() {
    closeWindow();
  }

  /** The window's suite request ends when its cluster shuts down. */
  public static synchronized void shutdownAutomatically(MiniSolrCloudCluster cluster) {
    if (cluster == windowOwner) {
      closeWindow();
    }
  }

  /**
   * The window opens with the first cluster or servers up: their client
   * registered ({@code <prefix>/client0}), recording started, and one
   * request on the thread that built them; a test already running (its
   * cluster built in its own {@code @Before}, after the runner began it)
   * gets its request now.
   */
  private static void openWindow(Object owner, String prefix, CloudSolrClient client) {
    windowOwner = owner;
    windowClient = client;
    registerClientNode(prefix + "/client0", client, client.getLbClient());
    MessageDoors.startRecording();
    suiteRequest = MessageDoors.beginRequest(client, "suite");
    if (testRunning) {
      beginTest();
    }
  }

  /**
   * The window's owner shuts down: the test's request ends first if it
   * still stands (a test that tears its cluster down in its own {@code
   * @After}, before the runner ends it), so the teardown is not recorded,
   * then the suite's.
   */
  private static void closeWindow() {
    if (windowOwner == null) {
      return;
    }
    endTest();
    MessageDoors.endRequest(suiteRequest, "suite");
    windowOwner = null;
    windowClient = null;
  }

  /** A client node: the client, its load balancer and, on ZooKeeper, its state reader and ZK client. */
  private static void registerClientNode(String name, BaseCloudSolrClient client, Object lbClient) {
    if (client.getClusterStateProvider() instanceof ZkClientClusterStateProvider) {
      MessageDoors.registerNode(client, "EXTERNAL_APP", "SOLR_CLIENT", name,
          lbClient, client.getZkStateReader(),
          client.getZkStateReader().getZkClient());
    } else {
      MessageDoors.registerNode(client, "EXTERNAL_APP", "SOLR_CLIENT", name, lbClient);
    }
  }

  /** {@code jettys} up and not yet named, named {@code <prefix>/node<i>} in the order of their homes. */
  private static void registerNodes(String prefix, List<JettySolrRunner> jettys) {
    List<JettySolrRunner> started = new ArrayList<>(jettys);
    started.sort(Comparator.comparing(JettySolrRunner::getSolrHome));
    for (JettySolrRunner jetty : started) {
      if (NODES.containsKey(jetty) || jetty.getCoreContainer() == null) {
        continue;
      }
      String name = prefix + "/node" + NODES.size();
      NODES.put(jetty, name);
      MessageDoors.registerSource(jetty, NODE, "SOLR_NODE", name, 0L);
      registerUp(jetty, name);
    }
  }

  /**
   * A registered node is up: the objects it runs its work through -- its container, its filter,
   * its handlers, its ZooKeeper client and state reader, its Overseer -- are the node, and so are
   * its names in this run only, its node name (host and port) and its ZooKeeper session, which
   * leader elections spell in the nodes they create.
   */
  private static void registerUp(JettySolrRunner jetty, String name) {
    CoreContainer cores = jetty.getCoreContainer();
    if (cores == null) {
      return;
    }
    ZkController zk = cores.getZkController();
    Object[] aliases = {cores, jetty.getSolrDispatchFilter(), cores.getUpdateShardHandler(),
        cores.getShardHandlerFactory(), zk, zk == null ? null : zk.getZkClient(),
        zk == null ? null : zk.getZkStateReader(), zk == null ? null : zk.getOverseer()};
    for (Object alias : aliases) {
      if (alias != null) {
        MessageDoors.registerSourceAlias(alias, jetty);
      }
    }
    if (zk != null) {
      MessageDoors.spelledAs(name, zk.getNodeName(),
          Long.toString(zk.getZkClient().getSolrZooKeeper().getSessionId()));
      // Its place among the nodes, whatever port this run gave it (CausynthRandom#order).
      CausynthRandom.ordinal(zk.getNodeName(),
          Integer.parseInt(name.substring(name.lastIndexOf("/node") + "/node".length())));
    }
  }

  /**
   * A jetty's start in the window is a node's from its first moment, what it starts before it
   * is up included: a registered node's restart at its next epoch, or a new node of the
   * registered cluster or servers whose ZooKeeper it joins, named the next node -- an
   * AbstractFullDistribZkTestBase test's leader brought back, a cluster's added node.  Any other
   * start (before the window, or of a cluster not yet registered) is left to the registration.
   */
  private static final class Starts implements JettySolrRunner.CausynthStarts {
    @Override
    public long starting(JettySolrRunner jetty, String zkHost) {
      synchronized (CausynthSolrCluster.class) {
        String name = NODES.get(jetty);
        if (name != null) {
          MessageDoors.restartSource(jetty, NODE, "SOLR_NODE", name,
              EPOCHS.merge(jetty, 1L, Long::sum));
        } else {
          String prefix = windowOwner == null ? null : PREFIXES.get(zkHost);
          if (prefix == null) {
            return 0L;
          }
          name = prefix + "/node" + NODES.size();
          NODES.put(jetty, name);
          MessageDoors.registerSource(jetty, NODE, "SOLR_NODE", name, 0L);
        }
      }
      return MessageDoors.beginNodeStartup(jetty);
    }

    @Override
    public void started(JettySolrRunner jetty, long startup) {
      if (startup == 0L) {
        return;
      }
      synchronized (CausynthSolrCluster.class) {
        registerUp(jetty, NODES.get(jetty));
      }
      MessageDoors.endNodeStartup(startup);
    }
  }
}
