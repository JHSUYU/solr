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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of solr-inj-03 (from solr-buggy-6101's): a shard split of a
 * collection with two replicas in a cluster that runs with legacyCloud=true.
 *
 * <p>The cluster property is set first and every node is let see it, so
 * every read of it inside the test sees legacy mode.  Then a collection is
 * made with its shard's leader on the first node and a second replica on the
 * second node (a 1x1 collection on the first node, then ADDREPLICA on the
 * second), four documents are indexed, and shard1 is split.  The split
 * creates each sub-shard's leader on the parent leader's node (the first) and,
 * since the shard has two replicas, one more replica of each sub-shard on the
 * other node (the second): SplitShardCmd first writes each such replica into
 * the collection's state itself (a placeholder), then has AddReplicaCmd create
 * its core without writing it into the state again
 * (SKIP_CREATE_REPLICA_IN_CLUSTER_STATE).  In legacy mode a core-admin CREATE
 * without a coreNodeName is valid (the node finds its replica's name by node
 * and core name), so the split succeeds.  Had the cluster run non-legacy, every
 * CREATE of the split would have had to name its replica's coreNodeName, or
 * CoreContainer.create would refuse it ("non legacy mode coreNodeName
 * missing").</p>
 *
 * <p>The collection is made through the Collections API, so in a non-legacy
 * world (every read of legacyCloud, node and Overseer, all through
 * CausynthLegacyCloud) the collection's own CREATE and the ADDREPLICA's name
 * their coreNodeName, and the first CREATEs the split could have refused are
 * the split's.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthSplitShardLegacy extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthSplitLegacy";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void splitUnderLegacyCloud() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "true").process(client);
    try {
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        ZkStateReader reader = jetty.getCoreContainer().getZkController().getZkStateReader();
        new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME).waitFor(
            "legacyCloud=true not seen by " + jetty.getNodeName(),
            () -> "true".equals(reader.getClusterProperties().get(ZkStateReader.LEGACY_CLOUD)));
      }

      // The roles are the workload's: the shard's leader on the first node, its second replica on the second.
      CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
          .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName()).process(client);
      cluster.waitForActiveCollection(COLLECTION, 1, 1);
      CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
          .setNode(cluster.getJettySolrRunner(1).getNodeName()).process(client);
      cluster.waitForActiveCollection(COLLECTION, 1, 2);
      for (int i = 0; i < 4; i++) {
        client.add(COLLECTION, new SolrInputDocument("id", "doc" + i));
      }
      client.commit(COLLECTION);

      CollectionAdminRequest.splitShard(COLLECTION).setShardName("shard1").process(client);
      waitForState("the sub-shards of shard1 are not active", COLLECTION, activeClusterShape(2, 6));

      client.commit(COLLECTION);
      assertEquals("every document in the sub-shards", 4,
          client.query(COLLECTION, new SolrQuery("*:*")).getResults().getNumFound());
    } finally {
      CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(client);
    }
  }
}
