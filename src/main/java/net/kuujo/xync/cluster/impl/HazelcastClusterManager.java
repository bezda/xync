/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.xync.cluster.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import net.kuujo.xync.cluster.ClusterManager;

import org.vertx.java.core.Handler;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

/**
 * Hazelcast-based cluster manager implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HazelcastClusterManager implements ClusterManager, MembershipListener {
  private final HazelcastInstance hazelcast;
  private final String nodeId;
  private Handler<String> joinHandler;
  private Handler<String> leaveHandler;

  public HazelcastClusterManager(HazelcastInstance hazelcast) {
    this.hazelcast = hazelcast;
    this.nodeId = hazelcast.getCluster().getLocalMember().getUuid();
    hazelcast.getCluster().addMembershipListener(this);
  }

  @Override
  public String getNodeId() {
    return nodeId;
  }

  @Override
  public Set<String> getNodes() {
    Set<String> nodes = new HashSet<>();
    for (Member member : hazelcast.getCluster().getMembers()) {
      nodes.add(member.getUuid());
    }
    return nodes;
  }

  @Override
  public void memberAdded(MembershipEvent event) {
    if (joinHandler != null) {
      joinHandler.handle(event.getMember().getUuid());
    }
  }

  @Override
  public void memberRemoved(MembershipEvent event) {
    if (leaveHandler != null) {
      leaveHandler.handle(event.getMember().getUuid());
    }
  }

  @Override
  public ClusterManager joinHandler(Handler<String> handler) {
    this.joinHandler = handler;
    return this;
  }

  @Override
  public ClusterManager leaveHandler(Handler<String> handler) {
    this.leaveHandler = handler;
    return this;
  }

  @Override
  public <K, V> Map<K, V> getMap(String name) {
    return hazelcast.getMap(name);
  }

  @Override
  public <T> Set<T> getSet(String name) {
    return hazelcast.getSet(name);
  }

  @Override
  public <T> List<T> getList(String name) {
    return hazelcast.getList(name);
  }

  @Override
  public <T> Queue<T> getQueue(String name) {
    return hazelcast.getQueue(name);
  }

}
