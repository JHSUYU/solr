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

import java.util.Collections;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of the Overseer's leaked collection lock (bug study #50) that
 * turns a collection read-only and back with an asynchronous request,
 * without the failure.
 *
 * <p>The Overseer's task processor takes the collection's lock for a task,
 * then marks the task running: an asynchronous task's id is written to the
 * running map in ZooKeeper.  Only a NodeExists or an interrupt there gives
 * the lock back; any other failure of that write leaves the lock taken and
 * the task in the running set, so the task is never dispatched and no later
 * task on the collection runs.  Here the asynchronous MODIFYCOLLECTION that
 * turns read-only off runs, and the add after it is applied.  Had its
 * running-map write failed (a ZooKeeper OperationTimeout after the write),
 * the collection would have stayed read-only and the add would have been
 * refused ("is read-only").</p>
 *
 * <p>The wait for the asynchronous task is a bounded number of status
 * requests, not a time bound and not an assertion: in a world where the task
 * never runs, the wait ends and the add is what fails.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthOverseerLockLeak extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthLock";
  private static final String REQUEST = "causynthLock-writable";
  private static final int STATUS_POLLS = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void addAfterAnAsyncReadOnlyOff() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName()).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    CollectionAdminRequest.modifyCollection(COLLECTION,
        Collections.singletonMap(ZkStateReader.READ_ONLY, "true")).process(client);
    // An empty value removes the property (SOLR-12507).
    CollectionAdminRequest.modifyCollection(COLLECTION,
        Collections.singletonMap(ZkStateReader.READ_ONLY, "")).processAsync(REQUEST, client);
    RequestStatusState state = RequestStatusState.NOT_FOUND;
    for (int poll = 0; poll < STATUS_POLLS; poll++) {
      state = CollectionAdminRequest.requestStatus(REQUEST).process(client).getRequestStatus();
      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        break;
      }
      Thread.sleep(1000);
    }

    client.add(COLLECTION, new SolrInputDocument("id", "1"));
    client.commit(COLLECTION);
    assertEquals("the asynchronous MODIFYCOLLECTION", RequestStatusState.COMPLETED, state);
    assertEquals(1, client.query(COLLECTION, new SolrQuery("*:*")).getResults().getNumFound());
  }
}
