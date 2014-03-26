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

import net.kuujo.xync.cluster.data.AsyncSet;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISet;

/**
 * Hazelcast set implementation.
 *
 * @author Jordan Halterman
 *
 * @param <T> The set type.
 */
public class HazelcastAsyncSet<T> implements AsyncSet<T> {
  private final String name;
  private final VertxInternal vertx;
  private final ISet<T> set;

  public HazelcastAsyncSet(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.set = hazelcast.getSet(name);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void add(final T value, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return set.add(value);
      }
    }, doneHandler);
  }

  @Override
  public void remove(final T value, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return set.remove(value);
      }
    }, doneHandler);
  }

  @Override
  public void contains(final Object value, Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return set.contains(value);
      }
    }, resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    vertx.executeBlocking(new Action<Integer>() {
      @Override
      public Integer perform() {
        return set.size();
      }
    }, resultHandler);
  }

  @Override
  public void isEmpty(Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return set.isEmpty();
      }
    }, resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        set.clear();
        return null;
      }
    }, doneHandler);
  }

}
