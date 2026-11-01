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
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of a shard split followed by an add (from solr-buggy-6101's
 * TestCausynthSplitShardLegacy, without its legacyCloud setting, in the
 * bug-check style: each failure returned, the wait bounded by a count).
 *
 * <p>One node: a 1x1 collection, four documents, SPLITSHARD of shard1, a wait
 * of at most 30 polls until the node no longer sees shard1 active, then one
 * more document and a commit.  With one replica per shard the split ends by a
 * final commit on the parent and the switch of the shards' states (parent
 * INACTIVE, sub-shards ACTIVE), so the client's later add is routed to a
 * sub-shard.  Had the parent been deactivated while the sub-shards were
 * removed (a failed split's cleanup), the client would find no active slice
 * for the document's hash ("No active slice servicing hash code").</p>
 */
@LuceneTestCase.Slow
public class TestCausynthSplitShardHash extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "causynthSplitHash";
  private static final int SHAPE_POLLS = 30;

  static {
    // GraphChecker: replica states in the collection's state.json, as Solr 8.11 keeps them unless a
    // collection asks for per-replica states (which the test framework picks at random).
    System.setProperty("use.perreplica", "false");
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void clearPerReplica() {
    System.clearProperty("use.perreplica");
  }

  @Test
  public void addAfterASplit() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    for (int i = 0; i < 4; i++) {
      client.add(COLLECTION, new SolrInputDocument("id", "doc" + i));
    }
    client.commit(COLLECTION);

    Exception split = null;
    try {
      CollectionAdminRequest.splitShard(COLLECTION).setShardName("shard1").process(client);
    } catch (Exception e) {
      log.warn("SPLITSHARD failed", e);
      split = e;
    }
    // GraphChecker: node0, the parent's host, must have seen the split's outcome before the add, which client0 routes to
    // the parent by the state it cached at its first add (CloudSolrClient keeps it 60 s): wait until node0's own view no
    // longer shows shard1 active (a finished split switches it to inactive; a split rolled back leaves it active, and
    // the wait ends at its bound)
    ZkStateReader parentNode = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getZkStateReader();
    for (int poll = 0; poll < SHAPE_POLLS && parentActive(parentNode); poll++) {
      Thread.sleep(1000);
    }

    Exception added = null;
    try {
      client.add(COLLECTION, new SolrInputDocument("id", "doc4"));
      client.commit(COLLECTION);
    } catch (Exception e) {
      log.warn("the add after the split failed", e);
      added = e;
    }

    assertNull("SPLITSHARD: " + split, split);
    assertNull("the add after the split: " + added, added);
    assertEquals("every document", 5, client.query(COLLECTION, new SolrQuery("*:*")).getResults().getNumFound());
  }

  /** Whether the node's own view still shows shard1, the split's parent, active. */
  private static boolean parentActive(ZkStateReader node) {
    DocCollection collection = node.getClusterState().getCollectionOrNull(COLLECTION);
    Slice parent = collection == null ? null : collection.getSlice("shard1");
    return parent == null || parent.getState() == Slice.State.ACTIVE;
  }
}
