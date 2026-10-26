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

import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of SOLR-9440 (solr-buggy-9440): FORCELEADER handled by a node
 * that hosts nothing of the collection but has watched it.
 *
 * <p>Two nodes: the collection (1x1) lives on the second; every collection
 * request goes to the first.  The first node's CollectionsHandler waits for
 * the new collection to be active (a state watch it drops once the replica is
 * active).  The second node then stops, so the shard's leader is DOWN, and
 * the first node is let see the second gone (one more watch of the
 * collection, dropped at once: its registration reads the state as it stands
 * now).  FORCELEADER on the first node finds the leader DOWN, so it is
 * applicable; no live replica can take over, and after its wait it answers
 * without a leader.</p>
 *
 * <p>Had the first node's view of the collection stayed at what its first
 * watch last saw -- the replica ACTIVE and leading -- FORCELEADER would refuse
 * ("The shard already has an active leader").</p>
 */
@LuceneTestCase.Slow
public class TestCausynthForceLeaderStaleView extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthForceLeaderStale";

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
    ZkStateReader firstReader = first.getCoreContainer().getZkController().getZkStateReader();
    try (HttpSolrClient admin = getHttpSolrClient(first.getBaseUrl().toString())) {
      CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
          .setCreateNodeSet(secondNode).process(admin);
      cluster.waitForActiveCollection(COLLECTION, 1, 1);

      cluster.stopJettySolrRunner(second);
      cluster.waitForJettyToStop(second);
      waitForState("the replica on the stopped node is not DOWN", COLLECTION,
          (liveNodes, collection) -> collection != null && collection.getReplicas().stream()
              .allMatch(replica -> replica.getState() == Replica.State.DOWN));

      firstReader.waitForState(COLLECTION, 60, TimeUnit.SECONDS,
          (liveNodes, collection) -> !liveNodes.contains(secondNode));

      CollectionAdminRequest.forceLeaderElection(COLLECTION, "shard1").process(admin);
    }
  }
}
