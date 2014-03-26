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

import net.kuujo.xync.cluster.data.AsyncList;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;

/**
 * Hazelcast list implementation.
 *
 * @author Jordan Halterman
 *
 * @param <T> The list type.
 */
public class HazelcastAsyncList<T> implements AsyncList<T> {
  private final String name;
  private final VertxInternal vertx;
  private final IList<T> list;

  public HazelcastAsyncList(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.list = hazelcast.getList(name);
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
        return list.add(value);
      }
    }, doneHandler);
  }

  @Override
  public void remove(final T value, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return list.remove(value);
      }
    }, doneHandler);
  }

  @Override
  public void contains(final Object value, Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return list.contains(value);
      }
    }, resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    vertx.executeBlocking(new Action<Integer>() {
      @Override
      public Integer perform() {
        return list.size();
      }
    }, resultHandler);
  }

  @Override
  public void isEmpty(Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return list.isEmpty();
      }
    }, resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        list.clear();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void get(final int index, Handler<AsyncResult<T>> resultHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return list.get(index);
      }
    }, resultHandler);
  }

  @Override
  public void set(final int index, final T value, Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        list.set(index, value);
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void remove(final int index, Handler<AsyncResult<T>> doneHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return list.remove(index);
      }
    }, doneHandler);
  }

}
