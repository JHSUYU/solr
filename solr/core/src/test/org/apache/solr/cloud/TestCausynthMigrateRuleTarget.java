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

import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of SOLR-18413 that exercises a MIGRATE routing rule, its
 * target collection's deletion and the rule's expiry, without the failure.
 *
 * <p>MIGRATE leaves a routing rule on the source shard naming the target
 * collection and an expiry the Overseer computed from its own clock.  Here
 * the rule is given one second, so by the time the target is deleted and the
 * next update of the route key reaches the source leader the rule has
 * expired: the leader takes the rule's expiry branch, removes the rule and
 * distributes the update to every replica.  Had the leader's clock stood
 * before the expiry, it would have looked the deleted target up after
 * applying the update locally, and the update would reach the leader only.</p>
 */
@LuceneTestCase.Slow
public class TestCausynthMigrateRuleTarget extends SolrCloudTestCase {
  private static final String SOURCE = "causynthMigrate-source";
  private static final String TARGET = "causynthMigrate-target";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void updateAfterRuleExpiryAndTargetDeletion() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(SOURCE, "conf", 1, 2).process(client);
    CollectionAdminRequest.createCollection(TARGET, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(SOURCE, 1, 2);
    cluster.waitForActiveCollection(TARGET, 1, 1);

    client.add(SOURCE, new SolrInputDocument("id", "a!1"));
    client.commit(SOURCE);

    RequestStatusState migrated = CollectionAdminRequest
        .migrateData(SOURCE, TARGET, "a!")
        .setForwardTimeout(1)
        .processAndWait(client, 300);
    assertEquals(RequestStatusState.COMPLETED, migrated);
    waitForState("MIGRATE left its routing rule on the source", SOURCE,
        (liveNodes, state) -> {
          if (state == null) return false;
          Map<String, RoutingRule> rules = state.getSlice("shard1").getRoutingRules();
          return rules != null && rules.containsKey("a!");
        });

    CollectionAdminRequest.deleteCollection(TARGET).process(client);
    waitForState("the target is deleted", TARGET, (liveNodes, state) -> state == null);

    client.add(SOURCE, new SolrInputDocument("id", "a!2"));
    client.commit(SOURCE);

    for (Replica replica : getCollectionState(SOURCE).getSlice("shard1")) {
      try (HttpSolrClient replicaClient = getHttpSolrClient(replica.getCoreUrl())) {
        SolrQuery query = new SolrQuery("id:\"a!2\"");
        query.set("distrib", false);
        assertEquals("a!2 on " + replica.getName(), 1,
            replicaClient.query(query).getResults().getNumFound());
      }
    }
  }
}
