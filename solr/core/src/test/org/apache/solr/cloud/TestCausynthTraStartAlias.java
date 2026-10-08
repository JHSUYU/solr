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

import java.time.Instant;
import java.time.format.DateTimeParseException;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of the time routed alias's start rewrite (bug study #20)
 * that creates a plain alias before the routed alias's first document,
 * without the failure.
 *
 * <p>A time routed alias created with date math in its start fixes the start
 * at its first document: the node that routes the document captures its view
 * of all aliases, and a task of its core writes that captured map back with
 * the start rewritten, over whatever ZooKeeper holds by then.  Here the plain
 * alias B is created before the first document and the routing node's view
 * already holds it, so the rewrite keeps B and a request via B is served.
 * Had the routing node's view not held B yet, the rewrite would have written
 * the aliases without B and the request via B would have found no collection
 * ("Collection not found: B").</p>
 *
 * <p>The start's date math names a fixed day, so the routed alias's
 * collections are the same in every run.  The routed alias's collection is
 * placed on the node the Overseer is not on: the Overseer applies its own
 * alias writes to its view as it makes them.  The workload never reads the
 * routing node's view of the aliases; it waits for the rewritten start in its
 * client's view, a bounded number of times.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthTraStartAlias extends SolrCloudTestCase {
  private static final String ROUTED = "causynthTra";
  private static final String PLAIN = "causynthTra-B";
  private static final String PLAIN_TARGET = "causynthTra-D";
  private static final String START = "2019-09-14T03:00:00Z/DAY";
  private static final String TIME_FIELD = "timestamp_dt";
  private static final int VIEW_POLLS = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void requestViaAnAliasCreatedBeforeTheFirstDocument() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String overseer = null;
    String other = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (jetty.getCoreContainer().getZkController().getOverseer().getUpdaterThread() != null) {
        overseer = jetty.getNodeName();
      } else {
        other = jetty.getNodeName();
      }
    }
    assertNotNull("the Overseer's node", overseer);
    assertNotNull("a node the Overseer is not on", other);

    CollectionAdminRequest.createCollection(PLAIN_TARGET, "conf", 1, 1)
        .setCreateNodeSet(overseer).process(client);
    cluster.waitForActiveCollection(PLAIN_TARGET, 1, 1);
    CollectionAdminRequest.createTimeRoutedAlias(ROUTED, START, "+1DAY", TIME_FIELD,
        CollectionAdminRequest.createCollection("_unused_", "conf", 1, 1).setCreateNodeSet(other))
        .process(client);
    cluster.waitForActiveCollection(ROUTED + TimeRoutedAlias.TYPE.getSeparatorPrefix() + "2019-09-14", 1, 1);

    CollectionAdminRequest.createAlias(PLAIN, PLAIN_TARGET).process(client);

    client.add(ROUTED, new SolrInputDocument("id", "1", TIME_FIELD, "2019-09-14T01:00:00Z"));
    client.commit(ROUTED);
    String start = START;
    for (int poll = 0; poll < VIEW_POLLS && !parses(start); poll++) {
      client.getZkStateReader().aliasesManager.update();
      start = client.getZkStateReader().getAliases().getCollectionAliasProperties(ROUTED)
          .get(TimeRoutedAlias.ROUTER_START);
      if (!parses(start)) {
        Thread.sleep(1000);
      }
    }

    long found = client.query(PLAIN, new SolrQuery("*:*")).getResults().getNumFound();
    assertTrue("the first document fixed the start: " + start, parses(start));
    assertEquals("nothing indexed via " + PLAIN, 0, found);
  }

  private static boolean parses(String start) {
    try {
      Instant.parse(start);
      return true;
    } catch (DateTimeParseException | NullPointerException e) {
      return false;
    }
  }
}
