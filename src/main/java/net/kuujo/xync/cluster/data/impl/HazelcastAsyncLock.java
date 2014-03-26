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

import java.util.concurrent.TimeUnit;

import net.kuujo.xync.cluster.data.AsyncLock;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

/**
 * Hazelcast lock implementation.
 *
 * @author Jordan Halterman
 */
public class HazelcastAsyncLock implements AsyncLock {
  private final String name;
  private final VertxInternal vertx;
  private final ILock lock;

  public HazelcastAsyncLock(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.lock = hazelcast.getLock(name);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void lock(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        lock.lock();
        return null;
      }
    }, doneHandler);
  }

  @Override
  public void tryLock(Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return lock.tryLock();
      }
    }, doneHandler);
  }

  @Override
  public void tryLock(final long timeout, Handler<AsyncResult<Boolean>> doneHandler) {
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        try {
          return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }, doneHandler);
  }

  @Override
  public void unlock(Handler<AsyncResult<Void>> doneHandler) {
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        lock.unlock();
        return null;
      }
    }, doneHandler);
  }

}
