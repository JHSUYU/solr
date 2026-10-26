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
 * The workload of SOLR-6101 (solr-buggy-6101): a shard split in a cluster
 * that runs with legacyCloud=true.
 *
 * <p>The cluster property is set first and every node is let see it, so
 * every read of it inside the test sees legacy mode.  Then a 1x1 collection
 * is created, four documents are indexed, and shard1 is split into two
 * sub-shards on the parent leader's node.  In legacy mode a core-admin CREATE
 * without a coreNodeName is valid (the node asks the Overseer to name the new
 * replica), so the split succeeds.  Had the node read legacyCloud=false at
 * the sub-shard core's creation, CoreContainer.create would refuse that
 * CREATE ("non legacy mode coreNodeName missing").</p>
 *
 * <p>The collection is made through the Collections API: in the bug's own
 * world, where every read of legacyCloud (node and Overseer, all through
 * CausynthLegacyCloud) says non-legacy, CreateCollectionCmd names the core's
 * coreNodeName, so the first CREATE the node refuses is the split's.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthSplitShardLegacy extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthSplitLegacy";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
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

      CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1).process(client);
      cluster.waitForActiveCollection(COLLECTION, 1, 1);
      for (int i = 0; i < 4; i++) {
        client.add(COLLECTION, new SolrInputDocument("id", "doc" + i));
      }
      client.commit(COLLECTION);

      CollectionAdminRequest.splitShard(COLLECTION).setShardName("shard1").process(client);
      waitForState("the sub-shards of shard1 are not active", COLLECTION, activeClusterShape(2, 3));

      client.commit(COLLECTION);
      assertEquals("every document in the sub-shards", 4,
          client.query(COLLECTION, new SolrQuery("*:*")).getResults().getNumFound());
    } finally {
      CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(client);
    }
  }
}
