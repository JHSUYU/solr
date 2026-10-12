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
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of the reindex whose abort cannot clean up (bug study #04)
 * that aliases a reindex's target, removes the alias and aborts the reindex,
 * without the failure.
 *
 * <p>REINDEXCOLLECTION S to T creates T, turns S read-only and copies the
 * documents; an abort makes the running reindex clean up: it deletes T, then
 * turns S writable again.  DELETE refuses a collection an alias names, and
 * the cleanup does not survive that refusal: S stays read-only.  Here the
 * reindex is held (TestInjection#reindexLatch) once S is read-only and T
 * exists; an operator aliases A to T, removes A, and aborts; the cleanup
 * deletes T and S takes the add after it.  Had the request removing A failed
 * (the operator logs it and goes on), A would still name T, the cleanup's
 * DELETE of T would be refused ("is part of aliases") and the add to S would
 * be refused too.</p>
 *
 * <p>Waits that follow a request that may fail in such a world are a bounded
 * number of status requests, never an assertion.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthReindexAbortAlias extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SOURCE = "causynthReindex";
  private static final String TARGET = "causynthReindex-target";
  private static final String ALIAS = "causynthReindex-A";
  private static final String REQUEST = "causynthReindex-start";
  private static final int POLLS = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void abortAfterAnAliasOfTheTargetIsRemoved() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(SOURCE, "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName()).process(client);
    cluster.waitForActiveCollection(SOURCE, 1, 1);
    client.add(SOURCE, new SolrInputDocument("id", "1"));
    client.commit(SOURCE);

    TestInjection.reindexLatch = new CountDownLatch(1);
    try {
      CollectionAdminRequest.reindexCollection(SOURCE).setTarget(TARGET).processAsync(REQUEST, client);
      waitForState("the reindex turned its source read-only", SOURCE,
          (liveNodes, state) -> state != null && state.isReadOnly());

      // Each alias step tolerates its failure, so a fault at either still
      // reaches the explicit abort below (a failed create must not end the
      // test while the reindex waits on its latch).
      try {
        CollectionAdminRequest.createAlias(ALIAS, TARGET).process(client);
      } catch (Exception e) {
        log.warn("creating alias {} failed", ALIAS, e);
      }
      try {
        CollectionAdminRequest.deleteAlias(ALIAS).process(client);
      } catch (Exception e) {
        log.warn("removing alias {} failed", ALIAS, e);
      }
      CollectionAdminRequest.reindexCollection(SOURCE).setCommand("abort").process(client);
    } finally {
      TestInjection.reindexLatch.countDown();
    }

    RequestStatusState state = RequestStatusState.NOT_FOUND;
    for (int poll = 0; poll < POLLS; poll++) {
      state = CollectionAdminRequest.requestStatus(REQUEST).process(client).getRequestStatus();
      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        break;
      }
      Thread.sleep(1000);
    }
    // The cleanup turns the source writable through the Overseer's state
    // queue: every node sees it a little after the reindex ends.
    for (int poll = 0; poll < POLLS && !writableEverywhere(); poll++) {
      Thread.sleep(1000);
    }

    client.add(SOURCE, new SolrInputDocument("id", "2"));
    client.commit(SOURCE);
    TestInjection.reindexLatch = null;
    assertEquals("the reindex ended", RequestStatusState.COMPLETED, state);
    assertEquals(2, client.query(SOURCE, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private static boolean writableEverywhere() {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      DocCollection source = jetty.getCoreContainer().getZkController().getZkStateReader()
          .getClusterState().getCollectionOrNull(SOURCE);
      if (source == null || source.isReadOnly()) {
        return false;
      }
    }
    return true;
  }
}
