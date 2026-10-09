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
package org.apache.solr.common.util;

import java.util.Random;

/**
 * A choice an automatic cluster ({@link CausynthBounds#AUTOMATIC}) makes the
 * same in the recording and in every replay: a {@link Random} seeded by the
 * test's seed ({@code tests.seed}), as Solr seeds its own where a test runs
 * (OverseerCollectionMessageHandler, HttpSolrCall); unseeded elsewhere.  A
 * client's shuffle of its nodes chose another node in a replay than in the
 * recording, and the work it started stood on another anchor
 * (solr-adhoc-31, round SG2: ForceLeaderTest's FORCELEADER).
 */
public final class CausynthRandom {
  private CausynthRandom() {
  }

  public static Random create() {
    String seed = System.getProperty("tests.seed");
    return CausynthBounds.automatic() && seed != null
        ? new Random(seed.hashCode()) : new Random();
  }
}
