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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of the RESTORE retry (bug study #26) that restores one
 * backup twice into one collection, without the failure.
 *
 * <p>A RESTORE into a new collection creates it with its shards in
 * CONSTRUCTION, restores each shard's core, applies its buffered updates and
 * only then marks the shards ACTIVE.  A RESTORE into a collection that exists
 * restores the cores and never touches the shards' state.  Here the first
 * RESTORE succeeds, so the second finds T's shards ACTIVE and the add to T is
 * routed.  Had a request of the first failed (a shard's RESTORECORE or its
 * buffered updates), T would have stayed in CONSTRUCTION, the retry would
 * still have reported success, and the add would have found no active shard
 * ("No active slice servicing hash code").</p>
 *
 * <p>Neither RESTORE's outcome is checked before the add, and nothing waits
 * for T's shards to be active: in a world where the first failed, the add is
 * what fails.</p>
 */
@LuceneTestCase.Slow
@LuceneTestCase.SuppressCodecs({"SimpleText"}) // a backup checks each file's footer checksum
public class TestCausynthRestoreRetry extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SOURCE = "causynthRestore-source";
  private static final String TARGET = "causynthRestore-target";
  private static final String BACKUP = "causynthRestore-backup";
  private static String location;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // The local repository writes the backup anywhere the nodes may.
    System.setProperty("solr.allowPaths", "*");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    location = createTempDir("causynthRestore").toAbsolutePath().toString();
  }

  @AfterClass
  public static void clearAllowPaths() {
    System.clearProperty("solr.allowPaths");
  }

  @Test
  public void restoreTwiceThenAdd() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(SOURCE, "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName()).process(client);
    cluster.waitForActiveCollection(SOURCE, 1, 1);
    client.add(SOURCE, new SolrInputDocument("id", "1"));
    client.add(SOURCE, new SolrInputDocument("id", "2"));
    client.commit(SOURCE);
    CollectionAdminRequest.backupCollection(SOURCE, BACKUP).setLocation(location).process(client);

    Exception first = restore(client);
    Exception retry = restore(client);

    client.add(TARGET, new SolrInputDocument("id", "3"));
    client.commit(TARGET);
    assertNull("the first RESTORE: " + first, first);
    assertNull("the second RESTORE: " + retry, retry);
    assertEquals("the backup's two documents and the add", 3,
        client.query(TARGET, new SolrQuery("*:*")).getResults().getNumFound());
  }

  /** One synchronous RESTORE of the backup into the target; its failure is returned, not thrown. */
  private static Exception restore(CloudSolrClient client) {
    try {
      CollectionAdminRequest.Restore restore =
          CollectionAdminRequest.restoreCollection(TARGET, BACKUP).setLocation(location);
      restore.setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName());
      restore.process(client);
      return null;
    } catch (Exception e) {
      log.warn("RESTORE into {} failed", TARGET, e);
      return e;
    }
  }
}
