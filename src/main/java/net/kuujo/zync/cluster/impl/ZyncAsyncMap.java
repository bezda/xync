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
package net.kuujo.zync.cluster.impl;

import java.util.Map;
import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;
import org.vertx.java.core.spi.cluster.AsyncMap;

/**
 * Asynchronous cluster map.
 *
 * @author Jordan Halterman
 *
 * @param <K> The map key type.
 * @param <V> The map value type.
 */
public class ZyncAsyncMap<K, V> implements AsyncMap<K, V> {
  private final VertxInternal vertx;
  private final Map<K, V> map;

  public ZyncAsyncMap(VertxInternal vertx, Map<K, V> map) {
    this.vertx = vertx;
    this.map = map;
  }

  @Override
  public void get(final K k, Handler<AsyncResult<V>> asyncResultHandler) {
    vertx.executeBlocking(new Action<V>() {
      public V perform() {
        return map.get(k);
      }
    }, asyncResultHandler);
  }

  @Override
  public void put(final K key, final V value, Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(new Action<Void>() {
      public Void perform() {
        map.put(key, value);
        return null;
      }
    }, completionHandler);
  }

  @Override
  public void remove(final K key, Handler<AsyncResult<Void>> completionHandler) {
    vertx.executeBlocking(new Action<Void>() {
      public Void perform() {
        map.remove(key);
        return null;
      }
    }, completionHandler);
  }

  public void containsKey(final K key, Handler<AsyncResult<Boolean>> asyncResultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return map.containsKey(key);
      }
    }, asyncResultHandler);
  }

  public void keySet(Handler<AsyncResult<Set<K>>> asyncResultHandler) {
    vertx.executeBlocking(new Action<Set<K>>() {
      @Override
      public Set<K> perform() {
        return map.keySet();
      }
    }, asyncResultHandler);
  }

}
