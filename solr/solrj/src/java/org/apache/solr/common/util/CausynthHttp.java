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

import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Function;

import edu.uva.liftlab.graphchecker.runtime.MessageDoors;

/**
 * Solr's HTTP transport on GraphChecker's message doors: one exchange per
 * HTTP request, its context in headers.
 *
 * <p>The client names the exchange (a correlation id of its own) and sends it
 * with the request; the node that serves the request answers under the same
 * id.  Four doors, one per leg and side: {@link #request} where a client
 * sends, {@link #received} where a node takes the request in, {@link
 * #respond} where the node writes its response, {@link #response} where the
 * client reads it.  A request is sent once: a retry, or another node tried
 * after this one, is a request of its own.</p>
 */
public final class CausynthHttp {
  public static final String TRANSPORT = "SOLR_HTTP";
  public static final String CORRELATION = "Causynth-Correlation";
  public static final String TRACE = "Causynth-Trace";
  public static final String SYMBOLIC = "Causynth-Symbolic";
  private static final String ATTEMPT = "1";

  private CausynthHttp() {
  }

  /**
   * A request leaves the calling thread, which no longer serves what it
   * received.
   *
   * @param owner  the client, whose registered node is the sender's when the
   *               thread names none
   * @param header sets one request header
   */
  public static void request(Object owner, BiConsumer<String, String> header) {
    MessageDoors.endInbound();
    String correlation = "solr-http-" + UUID.randomUUID();
    MessageDoors.Outbound outbound = MessageDoors.outbound(TRANSPORT,
        correlation, ATTEMPT, MessageDoors.REQUEST, owner, null);
    header.accept(CORRELATION, correlation);
    write(outbound, header);
  }

  /**
   * The response to a {@link #request} arrives at the thread that reads it;
   * the serving node named the exchange again ({@link #respond}).
   */
  public static void response(Function<String, String> header, Object owner) {
    String correlation = header.apply(CORRELATION);
    if (correlation == null || correlation.isEmpty()) {
      return;
    }
    MessageDoors.inbound(header.apply(TRACE), header.apply(SYMBOLIC), TRANSPORT,
        correlation, ATTEMPT, MessageDoors.RESPONSE, owner, false);
  }

  /**
   * A request arrives at the node {@code owner} belongs to; its exchange is
   * the thread's until {@link MessageDoors#endInbound(MessageDoors.Inbound)}.
   * A request from outside GraphChecker's doors names no exchange.
   *
   * @return the installed exchange; its {@code correlationId} is empty for a
   *         request that named none
   */
  public static MessageDoors.Inbound received(Function<String, String> header,
      Object owner) {
    String correlation = header.apply(CORRELATION);
    if (correlation == null || correlation.isEmpty()) {
      return MessageDoors.inbound(null, null, TRANSPORT, "", ATTEMPT,
          MessageDoors.REQUEST, owner, false);
    }
    return MessageDoors.inbound(header.apply(TRACE), header.apply(SYMBOLIC),
        TRANSPORT, correlation, ATTEMPT, MessageDoors.REQUEST, owner, false);
  }

  /** The node's response to the request {@code correlation} names leaves. */
  public static void respond(String correlation, BiConsumer<String, String> header,
      Object owner) {
    if (correlation == null || correlation.isEmpty()) {
      return;
    }
    header.accept(CORRELATION, correlation);
    write(MessageDoors.outbound(TRANSPORT, correlation, ATTEMPT,
        MessageDoors.RESPONSE, owner, null), header);
  }

  private static void write(MessageDoors.Outbound outbound,
      BiConsumer<String, String> header) {
    if (!outbound.active()) {
      return;
    }
    header.accept(TRACE, outbound.trace);
    if (!outbound.symbolic.isEmpty()) {
      header.accept(SYMBOLIC, outbound.symbolic);
    }
  }
}
