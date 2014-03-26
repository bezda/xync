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

import net.kuujo.xync.cluster.data.AsyncQueue;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;

/**
 * Hazelcast queue implementation.
 *
 * @author Jordan Halterman
 *
 * @param <T> The queue type.
 */
public class HazelcastAsyncQueue<T> implements AsyncQueue<T> {
  private final String name;
  private final VertxInternal vertx;
  private final IQueue<T> queue;

  public HazelcastAsyncQueue(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.queue = hazelcast.getQueue(name);
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
        return queue.add(value);
      }
    }, doneHandler);
  }

  @Override
  public void remove(final T value, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return queue.remove(value);
      }
    }, doneHandler);
  }

  @Override
  public void contains(final Object value, Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return queue.contains(value);
      }
    }, resultHandler);
  }

  @Override
  public void size(Handler<AsyncResult<Integer>> resultHandler) {
    vertx.executeBlocking(new Action<Integer>() {
      @Override
      public Integer perform() {
        return queue.size();
      }
    }, resultHandler);
  }

  @Override
  public void isEmpty(Handler<AsyncResult<Boolean>> resultHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return queue.isEmpty();
      }
    }, resultHandler);
  }

  @Override
  public void clear(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        queue.clear();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void offer(final T value, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return queue.offer(value);
      }
    }, doneHandler);
  }

  @Override
  public void element(Handler<AsyncResult<T>> resultHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return queue.element();
      }
    }, resultHandler);
  }

  @Override
  public void peek(Handler<AsyncResult<T>> resultHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return queue.peek();
      }
    }, resultHandler);
  }

  @Override
  public void poll(Handler<AsyncResult<T>> resultHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return queue.poll();
      }
    }, resultHandler);
  }

  @Override
  public void remove(Handler<AsyncResult<T>> resultHandler) {
    vertx.executeBlocking(new Action<T>() {
      @Override
      public T perform() {
        return queue.remove();
      }
    }, resultHandler);
  }

}
