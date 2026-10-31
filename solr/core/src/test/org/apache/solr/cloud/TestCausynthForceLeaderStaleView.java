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

import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of solr-inj-05 (from solr-buggy-9440's): FORCELEADER handled by
 * the node that handled the collection's CREATE, while that collection's only
 * replica is on a node that is stopping.
 *
 * <p>Two nodes: the collection (1x1) lives on the second; every collection
 * request goes to the first.  The first node's CollectionsHandler waits for
 * the new collection to be active.  The second node is then stopped, on a
 * thread of its own, and as soon as the test client sees the replica DOWN,
 * FORCELEADER goes to the first node; only then does the test wait for the
 * second node to have stopped.  FORCELEADER finds the leader DOWN in the first
 * node's view, so it is applicable; no live replica can take over, and after
 * its wait it answers without a leader.</p>
 *
 * <p>Before the stop the test waits (a bounded number of polls) until the
 * cluster is quiet: the second node live, its replica ACTIVE in a fresh read
 * of ZooKeeper, and no message left in the Overseer's state queue.  So the
 * replica's next DOWN is the stop's.  (Had the second node's ZooKeeper
 * session expired while it registered its core, it would publish itself DOWN
 * on reconnecting, then register again and publish its replica leader and
 * ACTIVE: a FORCELEADER sent at that transient DOWN would rightly be refused,
 * the leader being ACTIVE again in ZooKeeper.)</p>
 *
 * <p>The first node reads the collection as ZooKeeper has it now: either
 * lazily (a live fetch, if nothing on it watches the collection) or through
 * its watch, whose event for the DOWN state came with the client's.  Had the
 * first node read a watched view its watch had not refreshed yet, FORCELEADER
 * would have found the stopped replica still ACTIVE and leading, and refused
 * ("The shard already has an active leader").  The stop runs on its own
 * thread because stopping a node returns only once the node is down, while
 * the replica turns DOWN at the start of the stop.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthForceLeaderStaleView extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthForceLeaderStale";
  private static final int QUIET_POLLS = 60;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void forceLeaderOnAWatchingNode() throws Exception {
    JettySolrRunner first = cluster.getJettySolrRunner(0);
    JettySolrRunner second = cluster.getJettySolrRunner(1);
    String secondNode = second.getNodeName();
    try (HttpSolrClient admin = getHttpSolrClient(first.getBaseUrl().toString())) {
      CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
          .setCreateNodeSet(secondNode).process(admin);
      cluster.waitForActiveCollection(COLLECTION, 1, 1);
      awaitQuiet(secondNode);

      AtomicReference<Exception> stopFailure = new AtomicReference<>();
      Thread stopper = new Thread(() -> {
        try {
          cluster.stopJettySolrRunner(second);
        } catch (Exception e) {
          stopFailure.set(e);
        }
      }, "causynth-stop-second");
      stopper.start();
      waitForState("the replica on the stopped node is not DOWN", COLLECTION,
          (liveNodes, collection) -> collection != null && collection.getReplicas().stream()
              .allMatch(replica -> replica.getState() == Replica.State.DOWN));

      CollectionAdminRequest.forceLeaderElection(COLLECTION, "shard1").process(admin);

      stopper.join();
      assertNull("stopping the second node: " + stopFailure.get(), stopFailure.get());
      cluster.waitForJettyToStop(second);
    }
  }

  /** Until the node is live, every replica of the collection ACTIVE in ZooKeeper, and the Overseer's state queue empty. */
  private static void awaitQuiet(String node) throws Exception {
    SolrZkClient zk = cluster.getZkClient();
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    String live = ZkStateReader.LIVE_NODES_ZKNODE + "/" + node;
    for (int poll = 0; poll < QUIET_POLLS; poll++) {
      boolean wasLive = zk.exists(live, true);
      reader.forceUpdateCollection(COLLECTION);
      DocCollection collection = reader.getClusterState().getCollectionOrNull(COLLECTION);
      boolean active = collection != null && !collection.getReplicas().isEmpty()
          && collection.getReplicas().stream().allMatch(replica -> replica.getState() == Replica.State.ACTIVE);
      boolean drained = zk.getChildren("/overseer/queue", null, true).isEmpty();
      if (wasLive && active && drained && zk.exists(live, true)) {
        return;
      }
      Thread.sleep(500);
    }
    fail("the cluster did not quiet down before the stop");
  }
}
