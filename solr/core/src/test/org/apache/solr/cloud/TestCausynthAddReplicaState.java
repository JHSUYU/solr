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

import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of an ADDREPLICA on the node that hosts nothing of the
 * collection yet, in the bug-check style: each failure returned, every wait
 * bounded by a count.
 *
 * <p>Two nodes: a 1x1 collection is created on the second; ADDREPLICA then
 * puts a second replica of shard1 on the first.  In non-legacy mode (Solr
 * 8.11's default) the Overseer's AddReplicaCmd writes the replica into the
 * collection's state (DOWN) through the Overseer's state queue, waits to see
 * it there and names its coreNodeName on the core-admin CREATE it sends to
 * the first node.  The first node's CoreContainer.create refuses a CREATE
 * without a coreNodeName in non-legacy mode ("non legacy mode coreNodeName
 * missing").  Here the state write succeeds, the CREATE carries the name, and
 * the replica becomes active.</p>
 *
 * <p>Replica states live in the collection's state.json (use.perreplica
 * false), as Solr 8.11 keeps them unless a collection asks for per-replica
 * states, so the Overseer takes no per-replica-state path.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthAddReplicaState extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "causynthAddReplica";
  private static final int ACTIVE_POLLS = 30;

  static {
    // GraphChecker: replica states in the collection's state.json, as Solr 8.11 keeps them unless a
    // collection asks for per-replica states (which the test framework picks at random).
    System.setProperty("use.perreplica", "false");
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void clearPerReplica() {
    System.clearProperty("use.perreplica");
  }

  @Test
  public void addReplicaOnTheOtherNode() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String first = cluster.getJettySolrRunner(0).getNodeName();
    String second = cluster.getJettySolrRunner(1).getNodeName();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .setCreateNodeSet(second).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    Exception added = null;
    try {
      CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1").setNode(first).process(client);
    } catch (Exception e) {
      log.warn("ADDREPLICA on {} failed", first, e);
      added = e;
    }
    int active = 0;
    for (int poll = 0; poll < ACTIVE_POLLS && (active = activeReplicas(client)) < 2; poll++) {
      Thread.sleep(1000);
    }

    assertNull("ADDREPLICA: " + added, added);
    assertEquals("active replicas of " + COLLECTION, 2, active);
  }

  /** The replicas of the collection active in the client's view. */
  private static int activeReplicas(CloudSolrClient client) {
    ClusterState state = client.getZkStateReader().getClusterState();
    DocCollection collection = state.getCollectionOrNull(COLLECTION);
    if (collection == null) {
      return 0;
    }
    int active = 0;
    for (Replica replica : collection.getReplicas()) {
      if (replica.isActive(state.getLiveNodes())) {
        active++;
      }
    }
    return active;
  }
}
