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
package org.apache.solr.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of SOLR-18406 that exercises a follower's index fetch across
 * a leader commit, without the failure.
 *
 * <p>A PULL replica fetches generation G in several requests: indexversion,
 * filelist(G), then one filecontent(G) per file (the configset keeps every
 * commit's segment and no compound files, so G is many files).  Between two
 * of those requests the leader keeps G only by the reservation it renews at
 * each, and a leader commit meanwhile asks its deletion policy to delete G.
 * Here the follower is held after filelist(G) (the hook upstream added for
 * its own test) while the leader commits G+1; the configset reserves for an
 * hour, so the policy finds G reserved and every filecontent of G is served.
 * Had the leader's clock stood past the reservation at that commit, G would
 * be deleted and the follower's next filecontent of G refused.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthPullReplicaReservation extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthPull";
  private static final int COMMITS = 8;
  private static final int DOCS_PER_COMMIT = 20;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("causynth-replication"))
        .configure();
  }

  @Test
  public void pullReplicaFetchesAcrossALeaderCommit() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1, 0, 1)
        .process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 2);
    Replica pull = getCollectionState(COLLECTION).getSlice("shard1")
        .getReplicas(r -> r.getType() == Replica.Type.PULL).get(0);

    try (HttpSolrClient follower = getHttpSolrClient(pull.getCoreUrl())) {
      // The follower fetches when this workload says so, and only then.
      replication(follower, "disablepoll");
      int id = 0;
      for (int commit = 0; commit < COMMITS; commit++) {
        id = index(client, id);
        client.commit(COLLECTION);
      }
      final int inG = id;

      CountDownLatch listed = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      IndexFetcher.testWait = () -> {
        listed.countDown();
        try {
          resume.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return true;
      };
      try {
        replication(follower, "fetchindex");
        assertTrue("the follower listed G", listed.await(60, TimeUnit.SECONDS));
        // G+1 while the follower holds filelist(G): the leader's deletion
        // policy asks to delete G, and G's reservation keeps it.
        index(client, id);
        client.commit(COLLECTION);
      } finally {
        IndexFetcher.testWait = () -> true;
        resume.countDown();
      }

      SolrQuery all = new SolrQuery("*:*");
      all.set("distrib", false);
      TimeOut fetched = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      long found = -1;
      while (!fetched.hasTimedOut()) {
        found = follower.query(all).getResults().getNumFound();
        if (found == inG) break;
        Thread.sleep(200);
      }
      assertEquals("the follower installed G", inG, found);
    }
  }

  private static int index(CloudSolrClient client, int id) throws Exception {
    List<SolrInputDocument> docs = new ArrayList<>(DOCS_PER_COMMIT);
    for (int doc = 0; doc < DOCS_PER_COMMIT; doc++) {
      docs.add(new SolrInputDocument("id", Integer.toString(id++)));
    }
    client.add(COLLECTION, docs);
    return id;
  }

  private static void replication(HttpSolrClient core, String command) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command", command);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/replication");
    request.process(core);
  }
}
