package net.kuujo.xync.cluster.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import net.kuujo.xync.cluster.ClusterManager;

import org.vertx.java.core.Handler;
import org.vertx.java.core.shareddata.SharedData;

/**
 * Shared data based cluster manager.
 *
 * @author Jordan Halterman
 */
public class SharedDataClusterManager implements ClusterManager {
  private final String id = UUID.randomUUID().toString();
  private final SharedData data;
  @SuppressWarnings("rawtypes")
  private final Map<String, List> lists = new HashMap<>();
  @SuppressWarnings("rawtypes")
  private final Map<String, Queue> queues = new HashMap<>();

  public SharedDataClusterManager(SharedData data) {
    this.data = data;
  }

  @Override
  public String getNodeId() {
    return id;
  }

  @Override
  public Set<String> getNodes() {
    Set<String> nodes = new HashSet<>();
    nodes.add(id);
    return nodes;
  }

  @Override
  public ClusterManager joinHandler(Handler<String> handler) {
    return this;
  }

  @Override
  public ClusterManager leaveHandler(Handler<String> handler) {
    return this;
  }

  @Override
  public <K, V> Map<K, V> getMap(String name) {
    return data.getMap(name);
  }

  @Override
  public <T> Set<T> getSet(String name) {
    return data.getSet(name);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> getList(String name) {
    List<T> list = lists.get(name);
    if (list == null) {
      list = new SharedDataList<T>(data.<Integer, Object>getMap(name));
      lists.put(name, list);
    }
    return list;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Queue<T> getQueue(String name) {
    Queue<T> queue = queues.get(name);
    if (queue == null) {
      queue = new SharedDataQueue<T>(data.<Integer, Object>getMap(name));
      queues.put(name, queue);
    }
    return queue;
  }

}
