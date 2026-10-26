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

import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.cloud.ZkStateReader;

/**
 * GraphChecker: every read of the cluster property legacyCloud goes through one
 * field-backed value.  Each call reads the property exactly as
 * {@link Overseer#isLegacy} does, at the same moment, stores the answer in the
 * field of a fresh holder and returns that field: the same value, nothing shared
 * between calls, no ordering added.  A configuration a field holds is written by
 * GraphChecker at every read of that field, in every thread of the JVM (a
 * MiniSolrCloudCluster runs every node in one JVM, so on the node and on the
 * Overseer alike), where the return of a getter such as {@code isLegacy} is planted
 * only inside an occurrence that mints it.  Callers on the node (CoreContainer,
 * ZkController) and on the Overseer (CreateCollectionCmd, AddReplicaCmd,
 * OverseerCollectionMessageHandler, ReplicaMutator) call this instead of
 * {@code Overseer.isLegacy}.
 */
public final class CausynthLegacyCloud {
  private boolean legacyCloud;

  private CausynthLegacyCloud() {
  }

  public static boolean isLegacy(ZkStateReader stateReader) {
    CausynthLegacyCloud read = new CausynthLegacyCloud();
    read.legacyCloud = Overseer.isLegacy(stateReader);
    return read.legacyCloud;
  }

  public static boolean isLegacy(ClusterStateProvider clusterStateProvider) {
    CausynthLegacyCloud read = new CausynthLegacyCloud();
    read.legacyCloud = Overseer.isLegacy(clusterStateProvider);
    return read.legacyCloud;
  }
}
