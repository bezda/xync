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
package net.kuujo.xync.cluster;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.vertx.java.core.Handler;

/**
 * Cluster manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ClusterManager {

  /**
   * Returns the node ID.
   *
   * @return The node ID.
   */
  String getNodeId();

  /**
   * Returns a set of nodes currently in the cluster.
   *
   * @return A set of cluster members.
   */
  Set<String> getNodes();

  /**
   * Sets a handler to be called when a member joins the cluster.
   *
   * @param handler A handler to be called when a member joins the cluster.
   * @return The cluster manager.
   */
  ClusterManager joinHandler(Handler<String> handler);

  /**
   * Sets a handler to be called when a member leaves the cluster.
   *
   * @param handler A handler to be called when a member leaves the cluster.
   * @return The cluster manager.
   */
  ClusterManager leaveHandler(Handler<String> handler);

  /**
   * Returns a cluster-wide map.
   *
   * @param name The map name.
   * @return A cluster-wide map.
   */
  <K, V> Map<K, V> getMap(String name);

  /**
   * Returns a cluster-wide set.
   *
   * @param name The set name.
   * @return A cluster-wide set.
   */
  <T> Set<T> getSet(String name);

  /**
   * Returns a cluster-wide list.
   *
   * @param name The list name.
   * @return A cluster-wide list.
   */
  <T> List<T> getList(String name);

  /**
   * Returns a cluster-wide queue.
   *
   * @param name The queue name.
   * @return A cluster-wide queue.
   */
  <T> Queue<T> getQueue(String name);

}
