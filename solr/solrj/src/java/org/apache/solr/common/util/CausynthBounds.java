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

import java.util.concurrent.TimeUnit;

/**
 * The bounds of an automatic cluster: a cluster test run as a GraphChecker
 * workload as it is written ({@link #AUTOMATIC} set on its JVM), where no
 * bound sized for a native run expires -- a socket or a connect, a wait for
 * a cluster state, a wait for a request.  The recording runs on a JIT and
 * every replay, held to the recorded order, on an interpreter many times
 * slower: a bound that expires there is the replay's, not the system's.
 * What the system itself expires (a routing rule, a commit's reservation)
 * reads its clock and is not a bound here.
 */
public final class CausynthBounds {
  public static final String AUTOMATIC = "causynth.cluster.automatic";
  /** Longer than any case runs: a bound that cannot expire. */
  public static final int NEVER_MS = (int) TimeUnit.HOURS.toMillis(6);

  private CausynthBounds() {
  }

  public static boolean automatic() {
    return Boolean.getBoolean(AUTOMATIC);
  }

  /** A bound: the native one, or {@code NEVER_MS} in an automatic cluster. */
  public static long of(long bound, TimeUnit unit) {
    return automatic() ? unit.convert(NEVER_MS, TimeUnit.MILLISECONDS) : bound;
  }

  /** A bound in milliseconds: the native one, or none in an automatic cluster. */
  public static int millis(int bound) {
    return (int) of(bound, TimeUnit.MILLISECONDS);
  }
}
