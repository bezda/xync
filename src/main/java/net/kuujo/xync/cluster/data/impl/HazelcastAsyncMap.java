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
package net.kuujo.xync.cluster.data.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import net.kuujo.xync.cluster.data.MapEvent.Type;
import net.kuujo.xync.cluster.data.MapEvent;
import net.kuujo.xync.cluster.data.WatchableAsyncMap;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

/**
 * Hazelcast map implementation.
 *
 * @author Jordan Halterman
 *
 * @param <K> The map key type.
 * @param <V> The map value type.
 */
public class HazelcastAsyncMap<K, V> implements WatchableAsyncMap<K, V> {
  private final String name;
  private final VertxInternal vertx;
  private final IMap<K, V> map;
  private final IMap<K, String> watchers;

  public HazelcastAsyncMap(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.map = hazelcast.getMap(name);
    this.watchers = hazelcast.getMap(String.format("%s.__watchers", name));
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void put(final K key, final V value, Handler<AsyncResult<V>> doneHandler) {
    vertx.executeBlocking(new Action<V>() {
      @Override
      public V perform() {
        V result = map.put(key, value);
        if (result != null) {
          triggerEvent(key, MapEvent.Type.CREATE, value);
        } else {
          triggerEvent(key, MapEvent.Type.UPDATE, value);
        }
        return result;
      }
    }, doneHandler);
  }

  @Override
  public void get(final K key, Handler<AsyncResult<V>> resultHandler) {
    vertx.executeBlocking(new Action<V>() {
      @Override
      public V perform() {
        return map.get(key);
      }
    }, resultHandler);
  }

  @Override
  public void remove(final K key, Handler<AsyncResult<V>> resultHandler) {
    vertx.executeBlocking(new Action<V>() {
      @Override
      public V perform() {
        V value = map.remove(key);
        triggerEvent(key, MapEvent.Type.DELETE, value);
        return value;
      }
    }, resultHandler);
  }

  @Override
  public void containsKey(final K key, Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return map.containsKey(key);
      }
    }, resultHandler);
  }

  @Override
  public void keySet(Handler<AsyncResult<Set<K>>> resultHandler) {
    vertx.executeBlocking(new Action<Set<K>>() {
      @Override
      public Set<K> perform() {
        return map.keySet();
      }
    }, resultHandler);
  }

  @Override
  public void values(Handler<AsyncResult<Collection<V>>> resultHandler) {
    vertx.executeBlocking(new Action<Collection<V>>() {
      @Override
      public Collection<V> perform() {
        return map.values();
      }
    }, resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    vertx.executeBlocking(new Action<Integer>() {
      @Override
      public Integer perform() {
        return map.size();
      }
    }, resultHandler);
  }

  @Override
  public void isEmpty(Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return map.isEmpty();
      }
    }, resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        map.clear();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void watch(final K key, final Type event, final String address, final Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<String>() {
      @Override
      public String perform() {
        return watchers.get(key);
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String swatchers = result.result();
          final JsonObject jsonWatchers = swatchers != null ? new JsonObject(swatchers) : new JsonObject();

          if (!jsonWatchers.containsField(address)) {
            jsonWatchers.putArray(address, new JsonArray());
          }

          // Validate that all provided events are valid.
          JsonArray jsonWatcher = jsonWatchers.getArray(address);

          // Only add the event if it doesn't already exist.
          if (!jsonWatcher.contains(event)) {
            jsonWatcher.add(event.toString());
            vertx.executeBlocking(new Action<Void>() {
              @Override
              public Void perform() {
                watchers.put(key, jsonWatchers.encode());
                return null;
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                } else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            });
          }
          else {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          }
        }
      }
    });
  }

  @Override
  public void unwatch(final K key, final Type event, final String address, final Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<String>() {
      @Override
      public String perform() {
        return watchers.get(key);
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String swatchers = result.result();
          final JsonObject jsonWatchers = swatchers != null ? new JsonObject(swatchers) : new JsonObject();

          // If the watcher doesn't exist then simply return ok.
          if (!jsonWatchers.containsField(address)) {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
            return;
          }

          // Validate that all provided events are valid.
          JsonArray jsonWatcher = jsonWatchers.getArray(address);

          // Only remove the event if it actually exists.
          if (jsonWatcher.contains(event.toString())) {
            Iterator<Object> iter = jsonWatcher.iterator();
            while (iter.hasNext()) {
              if (iter.next().equals(event.toString())) {
                iter.remove();
              }
            }

            vertx.executeBlocking(new Action<Void>() {
              @Override
              public Void perform() {
                watchers.put(key, jsonWatchers.encode());
                return null;
              }
            }, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                } else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            });
          }
          else {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          }
        }
      }
    });
  }

  private void triggerEvent(final K key, final MapEvent.Type event, final V value) {
    vertx.executeBlocking(new Action<String>() {
      @Override
      public String perform() {
        return watchers.get(key);
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.succeeded() && result.result() != null) {
          JsonObject message = new JsonObject()
              .putString("event", event.toString())
              .putValue("key", key)
              .putValue("value", value);
          JsonObject change = new JsonObject()
              .putString("event", MapEvent.Type.CHANGE.toString())
              .putValue("key", key)
              .putValue("value", value);

          JsonObject jsonWatchers = new JsonObject(result.result());
          for (String address : jsonWatchers.getFieldNames()) {
            JsonArray jsonWatcher = jsonWatchers.getArray(address);
            if (jsonWatcher.contains(event.toString())) {
              vertx.eventBus().send(address, message);
              vertx.eventBus().send(address, change);
            }
          }
        }
      }
    });
  }

}
