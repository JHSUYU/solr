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
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The workload of a time routed alias whose first document lies past its head
 * collection's range, so the routing node creates the next collection
 * synchronously (from TestCausynthTraStartAlias, in the bug-check style: the
 * add's failure returned).
 *
 * <p>The routed alias starts on a fixed day, 2019-09-14, with one collection
 * a day; its first collection is placed on the node the Overseer is not on,
 * which then routes the alias's documents.  A plain alias of the head
 * collection is created next (another version of the aliases, which the
 * routing node's alias watcher takes in), and half a second later the one
 * document, stamped 2019-09-15, past the head's range: the routing node's
 * update processor asks the Overseer to create the alias's next collection
 * (MAINTAINROUTEDALIAS: the collection, then the alias's new collection list
 * written to /aliases.json) and, once the Overseer answered, checks that its
 * own view of the alias lists the new collection (RoutedAlias#doSynchronous).
 * Here the node refreshes its view of the aliases from ZooKeeper after the
 * Overseer answered, so the check passes and the document lands in the new
 * collection.  Had the node checked a view that its alias watcher had not
 * refreshed yet, the check would have failed ("After we attempted to create
 * ... it did not exist").</p>
 *
 * <p>The names are as short as Solr takes them (the alias "t", its field "ts",
 * the plain alias "b"): the aliases' content is what a re-paired read of the
 * routing node's view carries.  The start names a fixed instant (no date
 * math), so the start rewrite never runs, and the collections are the same in
 * every run.  Replica states
 * live in the collection's state.json (use.perreplica false).</p>
 */
@LuceneTestCase.Slow
public class TestCausynthTraSyncCreate extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String ROUTED = "t";
  private static final String START = "2019-09-14T00:00:00Z";
  private static final String TIME_FIELD = "ts";
  private static final String HEAD = ROUTED + TimeRoutedAlias.TYPE.getSeparatorPrefix() + "2019-09-14";
  private static final String NEXT = ROUTED + TimeRoutedAlias.TYPE.getSeparatorPrefix() + "2019-09-15";
  private static final String PLAIN = "b";

  static {
    // GraphChecker: replica states in the collection's state.json, as Solr 8.11 keeps them unless a
    // collection asks for per-replica states (which the test framework picks at random).
    System.setProperty("use.perreplica", "false");
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void clearPerReplica() {
    System.clearProperty("use.perreplica");
  }

  @Test
  public void documentPastTheHeadCollection() throws Exception {
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

    CollectionAdminRequest.createTimeRoutedAlias(ROUTED, START, "+1DAY", TIME_FIELD,
        CollectionAdminRequest.createCollection("_unused_", "conf", 1, 1).setCreateNodeSet(other))
        .process(client);
    cluster.waitForActiveCollection(HEAD, 1, 1);

    CollectionAdminRequest.createAlias(PLAIN, HEAD).process(client);
    Thread.sleep(500);

    Exception added = add(client, "1", "2019-09-15T01:00:00Z");

    assertNull("the add past the head collection: " + added, added);
    client.commit(NEXT);
    assertEquals("the document in " + NEXT, 1,
        client.query(NEXT, new SolrQuery("*:*")).getResults().getNumFound());
  }

  /** One add to the routed alias; its failure is returned, not thrown. */
  private static Exception add(CloudSolrClient client, String id, String timestamp) {
    try {
      client.add(ROUTED, new SolrInputDocument("id", id, TIME_FIELD, timestamp));
      return null;
    } catch (Exception e) {
      log.warn("the add of {} at {} failed", id, timestamp, e);
      return e;
    }
  }
}
