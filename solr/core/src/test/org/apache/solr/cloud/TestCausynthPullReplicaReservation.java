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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of SOLR-18406 that exercises a PULL replica's index fetch
 * while its leader keeps committing, without the failure.
 *
 * <p>The follower fetches one generation G in many requests (indexversion,
 * filelist, one filecontent per file; the configset keeps every commit's
 * segment and no compound files, so G is many files).  Between two of those
 * requests the leader keeps G only by a reservation it renews at each one,
 * and every commit the leader makes meanwhile asks its deletion policy to
 * delete G.  The configset reserves for an hour, so the policy always finds G
 * reserved and every filecontent of G is served.  Had the leader's clock
 * stood past the reservation at one of those commits, G would be deleted and
 * the follower's next filecontent of G refused.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthPullReplicaReservation extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthPull";
  private static final int COMMITS = 24;
  private static final int DOCS_PER_COMMIT = 20;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("causynth-replication"))
        .configure();
  }

  @Test
  public void pullReplicaFetchesWhileLeaderCommits() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1, 0, 1)
        .process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    int id = 0;
    for (int commit = 0; commit < COMMITS; commit++) {
      List<SolrInputDocument> docs = new ArrayList<>(DOCS_PER_COMMIT);
      for (int doc = 0; doc < DOCS_PER_COMMIT; doc++) {
        docs.add(new SolrInputDocument("id", Integer.toString(id++)));
      }
      client.add(COLLECTION, docs);
      client.commit(COLLECTION);
      Thread.sleep(100);
    }

    Replica pull = getCollectionState(COLLECTION).getSlice("shard1")
        .getReplicas(r -> r.getType() == Replica.Type.PULL).get(0);
    SolrQuery all = new SolrQuery("*:*");
    all.set("distrib", false);
    TimeOut caughtUp = new TimeOut(120, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    long found = -1;
    try (HttpSolrClient pullClient = getHttpSolrClient(pull.getCoreUrl())) {
      while (!caughtUp.hasTimedOut()) {
        found = pullClient.query(all).getResults().getNumFound();
        if (found == id) break;
        Thread.sleep(500);
      }
    }
    assertEquals("documents on the PULL replica", id, found);
  }
}
