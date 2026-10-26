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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of SOLR-9440 (solr-buggy-9440): FORCELEADER handled by a node
 * that hosts nothing of the collection but has watched it.
 *
 * <p>Two nodes: the collection (1x1) lives on the second; every collection
 * request goes to the first.  The first node's CollectionsHandler waits for
 * the new collection to be active (a state watch it drops once the replica is
 * active).  The second node then stops, so the shard's leader is DOWN.
 * FORCELEADER on the first node finds the leader DOWN, so it is applicable;
 * no live replica can take over, and after its wait it answers without a
 * leader.</p>
 *
 * <p>The first node's watch is satisfied at its registration (the replica is
 * already active when the CREATE returns), and a registration refresh
 * publishes twice (per-replica states, then the state): the second publish,
 * of a collection no longer watched, drops the entry, so the first node reads
 * the collection lazily.  Had the replica gone active only after the first
 * node registered its watch, the watch would end on that event's single
 * publication and -- with SOLR-9440's bug -- the first node would keep the
 * replica ACTIVE and leading for good: FORCELEADER would refuse ("The shard
 * already has an active leader").</p>
 *
 * <p>GraphChecker: a workaround (Overseer#failedStateWriteRetryDelayMs, 5 s
 * here): after a state message's ZooKeeper write fails, the Overseer waits
 * before it refreshes and re-processes that message, so the messages queued
 * meanwhile (the replica's ACTIVE) are written after the wait.  With no failed
 * write nothing waits and the run is the one above.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthForceLeaderStaleView extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthForceLeaderStale";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // GraphChecker: the workaround above, test-only.
    Overseer.failedStateWriteRetryDelayMs = 5000;
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void resetOverseerRetryDelay() {
    Overseer.failedStateWriteRetryDelayMs = 0;
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

      cluster.stopJettySolrRunner(second);
      cluster.waitForJettyToStop(second);
      waitForState("the replica on the stopped node is not DOWN", COLLECTION,
          (liveNodes, collection) -> collection != null && collection.getReplicas().stream()
              .allMatch(replica -> replica.getState() == Replica.State.DOWN));

      CollectionAdminRequest.forceLeaderElection(COLLECTION, "shard1").process(admin);
    }
  }
}
