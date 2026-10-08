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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of the MIGRATE buffer leak (bug study #17) that migrates two
 * route keys into one target, without the failure.
 *
 * <p>MIGRATE first asks the target leader to buffer updates
 * (REQUESTBUFFERUPDATES: its update log goes ACTIVE to BUFFERING), then
 * publishes the routing rule, splits the key off the source into a temporary
 * collection, merges it into the target, and only at its end asks the target
 * to apply the buffered updates (back to ACTIVE).  Nothing between undoes the
 * buffering when a step fails.  Here both MIGRATEs succeed, so the second
 * one's buffer request finds the target ACTIVE.  Had a step of the first
 * failed after its buffer request (a request to a node, a ZooKeeper write),
 * the target would have stayed BUFFERING and the second MIGRATE's buffer
 * request would have been refused ("not in active state").</p>
 *
 * <p>The first MIGRATE's outcome is not checked before the second: in a world
 * where it failed, the second is the one that fails.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthMigrateBufferLeak extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SOURCE = "causynthBuffer-source";
  private static final String TARGET = "causynthBuffer-target";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void twoMigratesIntoOneTarget() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    // The roles are the workload's: the source alone on the first node, the
    // target alone on the second.
    CollectionAdminRequest.createCollection(SOURCE, "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName()).process(client);
    CollectionAdminRequest.createCollection(TARGET, "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName()).process(client);
    cluster.waitForActiveCollection(SOURCE, 1, 1);
    cluster.waitForActiveCollection(TARGET, 1, 1);

    client.add(SOURCE, new SolrInputDocument("id", "a!1"));
    client.add(SOURCE, new SolrInputDocument("id", "b!1"));
    client.commit(SOURCE);

    Exception first = migrate(client, "a!");
    Exception second = migrate(client, "b!");

    assertNull("the first MIGRATE: " + first, first);
    assertNull("the second MIGRATE: " + second, second);
    client.commit(TARGET);
    assertEquals("both keys on the target", 2,
        client.query(TARGET, new SolrQuery("*:*")).getResults().getNumFound());
  }

  /** One synchronous MIGRATE of the key; its failure is returned, not thrown. */
  private static Exception migrate(CloudSolrClient client, String splitKey) {
    try {
      CollectionAdminRequest.migrateData(SOURCE, TARGET, splitKey).process(client);
      return null;
    } catch (Exception e) {
      log.warn("MIGRATE of {} failed", splitKey, e);
      return e;
    }
  }
}
