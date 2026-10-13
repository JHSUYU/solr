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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The workload of the snapshot that skips a replica (bug study #16) that
 * backs a collection up from a snapshot after the snapshot's leader replica
 * is gone, without the failure.
 *
 * <p>CREATESNAPSHOT sends its request only to the replicas its Overseer sees
 * ACTIVE and records the snapshot as successful as long as no request it sent
 * failed.  BACKUP with the snapshot's name backs each shard up from a live
 * replica that holds the snapshot.  The shard's leader is on the Overseer's
 * node; its second replica is added on the other node, the Overseer waiting
 * for its entry only, and the client waits until its own view shows the
 * replica ACTIVE before the snapshot.  Here the Overseer's view has caught up
 * by then, so after the leader replica is deleted the other one still holds
 * the snapshot and the backup succeeds.  Had the Overseer's view at
 * CREATESNAPSHOT still shown the added replica recovering -- nothing on its
 * node waits for its watch to deliver that state -- the snapshot would have
 * held the leader only, still be recorded successful, and the backup would
 * have found no live replica with it ("Unable to find any live replica with a
 * snapshot named").  (A collection created with both replicas cannot show it:
 * CREATE and the leader's own registration wait on the Overseer's view.)</p>
 */
@LuceneTestCase.Slow
@LuceneTestCase.SuppressCodecs({"SimpleText"}) // a backup checks each file's footer checksum
public class TestCausynthSnapshotBackup extends SolrCloudTestCase {
  private static final String COLLECTION = "causynthSnapshot";
  private static final String SNAPSHOT = "causynthSnapshot-M";
  private static String location;

  static {
    // GraphChecker: replica states in the collection's state.json, as Solr 8.11 keeps them unless a
    // collection asks for per-replica states (which the test framework picks at random): a replica's
    // state change is then a version of the state every node's view re-points to, not a child node.
    System.setProperty("use.perreplica", "false");
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    // The local repository writes the backup anywhere the nodes may.
    System.setProperty("solr.allowPaths", "*");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    location = createTempDir("causynthSnapshot").toAbsolutePath().toString();
  }

  @AfterClass
  public static void clearAllowPaths() {
    System.clearProperty("solr.allowPaths");
    System.clearProperty("use.perreplica");
  }

  @Test
  public void backupFromASnapshotAfterItsLeaderIsGone() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String overseer = OverseerTaskProcessor.getLeaderNode(
        client.getZkStateReader().getZkClient());
    String other = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (!jetty.getNodeName().equals(overseer)) other = jetty.getNodeName();
    }
    // One shard, its leader on the Overseer's node.
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .setCreateNodeSet(overseer).setMaxShardsPerNode(1).process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    // The documents go in before the second replica, which recovers them from
    // the leader: an update carries the client's view of the collection
    // (_stateVer_), and one sent after the client saw the new replica ACTIVE
    // makes the Overseer's node refresh its view to that version
    // (ZkStateReader#compareStateVersions), so between ADDREPLICA and
    // CREATESNAPSHOT nothing but the node's own watch moves its view, as in
    // bug study #16.
    client.add(COLLECTION, new SolrInputDocument("id", "1"));
    client.add(COLLECTION, new SolrInputDocument("id", "2"));
    client.commit(COLLECTION);
    // A second replica on the other node, with no final state to wait for:
    // the client waits for it ACTIVE in its own view.
    CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1").setNode(other)
        .process(client);
    cluster.waitForActiveCollection(COLLECTION, 1, 2);

    new CollectionAdminRequest.CreateSnapshot(COLLECTION, SNAPSHOT).process(client);

    Replica leader = getCollectionState(COLLECTION).getSlice("shard1").getLeader();
    CollectionAdminRequest.deleteReplica(COLLECTION, "shard1", leader.getName()).process(client);
    // The other replica leads the shard alone.
    waitForState("the snapshot's leader replica is gone", COLLECTION, (liveNodes, state) -> {
      if (state == null) return false;
      Slice shard = state.getSlice("shard1");
      Replica now = shard.getLeader();
      return shard.getReplica(leader.getName()) == null && now != null
          && now.isActive(liveNodes);
    });

    CollectionAdminRequest.backupCollection(COLLECTION, "causynthSnapshot-backup")
        .setLocation(location).setCommitName(SNAPSHOT).setIncremental(false)
        .process(client);
  }
}
