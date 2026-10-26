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
 * The workload of SOLR-6197 (solr-buggy-6197): one MIGRATE in a cluster that
 * runs with legacyCloud=true.
 *
 * <p>The cluster property is set first and every node is let see it, so
 * every read of it inside the test sees legacy mode.  The source collection
 * lives alone on the first node, the target alone on the second (the shape of
 * TestCausynthMigrateBufferLeak); two documents of route key a! are indexed
 * into the source and migrated to the target.  MIGRATE splits the key into a
 * temporary collection on the source leader's node and creates that
 * collection's second replica on the target leader's node with a core-admin
 * CREATE.  In legacy mode a CREATE without a coreNodeName is valid (the node
 * asks the Overseer to name the replica), so the MIGRATE succeeds.  Had the
 * target node read legacyCloud=false at that core's creation,
 * CoreContainer.create would refuse it ("non legacy mode coreNodeName
 * missing").</p>
 *
 * <p>The collections are made through the Collections API: in the bug's own
 * world, where every read of legacyCloud (node and Overseer, all through
 * CausynthLegacyCloud) says non-legacy, CreateCollectionCmd names each core's
 * coreNodeName (the source, the target and MIGRATE's temporary collection), so
 * the first CREATE a node refuses is MIGRATE's bare one on the target node.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthMigrateLegacy extends SolrCloudTestCase {
  private static final String SOURCE = "causynthMigrateLegacy-source";
  private static final String TARGET = "causynthMigrateLegacy-target";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void migrateUnderLegacyCloud() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "true").process(client);
    try {
      for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
        ZkStateReader reader = jetty.getCoreContainer().getZkController().getZkStateReader();
        new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME).waitFor(
            "legacyCloud=true not seen by " + jetty.getNodeName(),
            () -> "true".equals(reader.getClusterProperties().get(ZkStateReader.LEGACY_CLOUD)));
      }

      CollectionAdminRequest.createCollection(SOURCE, "conf", 1, 1)
          .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName()).process(client);
      CollectionAdminRequest.createCollection(TARGET, "conf", 1, 1)
          .setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName()).process(client);
      cluster.waitForActiveCollection(SOURCE, 1, 1);
      cluster.waitForActiveCollection(TARGET, 1, 1);

      client.add(SOURCE, new SolrInputDocument("id", "a!1"));
      client.add(SOURCE, new SolrInputDocument("id", "a!2"));
      client.commit(SOURCE);

      CollectionAdminRequest.migrateData(SOURCE, TARGET, "a!").process(client);

      client.commit(TARGET);
      assertEquals("the key's documents on the target", 2,
          client.query(TARGET, new SolrQuery("*:*")).getResults().getNumFound());
    } finally {
      CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(client);
    }
  }
}
