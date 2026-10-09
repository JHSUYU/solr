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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

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
  /** Per node ({@code host:port}), the ordinal the cluster adapter numbered it by. */
  private static final Map<String, Integer> ORDINALS = new ConcurrentHashMap<>();

  private CausynthRandom() {
  }

  /**
   * The cluster adapter numbered node {@code nodeName} ({@code
   * host:port_context}, as live nodes name it) {@code ordinal}: its place in
   * the order the cluster made its nodes, alike in every run, whatever port
   * the run gave it.
   */
  public static void ordinal(String nodeName, int ordinal) {
    ORDINALS.put(hostPort(nodeName), ordinal);
  }

  /**
   * {@code list} -- nodes, their URLs or replicas, {@code node} naming each
   * one's node -- in an automatic cluster put in the order of its nodes'
   * ordinals ({@link #ordinal}; a node no adapter numbered last, by name),
   * before a choice is drawn from it or it is tried in order: built from a
   * set keyed by ports, its order is no other run's (solr-adhoc-10, round
   * PE3: a cloud client's requests went to node0 in the replay, to node2 in
   * the recording, with one seeded draw).  Left as it is elsewhere.
   */
  public static <T> void order(List<T> list, Function<? super T, String> node) {
    if (!CausynthBounds.automatic() || list.size() < 2) return;
    list.sort(Comparator.<T>comparingInt(each -> ordinalOf(node.apply(each)))
        .thenComparing(each -> String.valueOf(node.apply(each))));
  }

  /** {@link #order}, then shuffled by {@code random}. */
  public static <T> void shuffle(List<T> list, Random random, Function<? super T, String> node) {
    order(list, node);
    Collections.shuffle(list, random);
  }

  private static int ordinalOf(String nodeOrUrl) {
    Integer ordinal = nodeOrUrl == null ? null : ORDINALS.get(hostPort(nodeOrUrl));
    return ordinal == null ? Integer.MAX_VALUE : ordinal;
  }

  /** {@code host:port} of a node name ({@code host:port_context}) or a URL. */
  private static String hostPort(String nodeOrUrl) {
    int scheme = nodeOrUrl.indexOf("://");
    if (scheme >= 0) {
      String rest = nodeOrUrl.substring(scheme + 3);
      int slash = rest.indexOf('/');
      return slash < 0 ? rest : rest.substring(0, slash);
    }
    int context = nodeOrUrl.indexOf('_');
    return context < 0 ? nodeOrUrl : nodeOrUrl.substring(0, context);
  }

  public static Random create() {
    String seed = System.getProperty("tests.seed");
    return CausynthBounds.automatic() && seed != null
        ? new Random(seed.hashCode()) : new Random();
  }
}
