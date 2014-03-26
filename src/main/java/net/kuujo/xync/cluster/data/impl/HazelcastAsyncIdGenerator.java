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

import net.kuujo.xync.cluster.data.AsyncIdGenerator;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.Action;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

/**
 * Hazelcast ID generator implementation.
 *
 * @author Jordan Halterman
 */
public class HazelcastAsyncIdGenerator implements AsyncIdGenerator {
  private final String name;
  private final VertxInternal vertx;
  private final IdGenerator id;

  public HazelcastAsyncIdGenerator(String name, VertxInternal vertx, HazelcastInstance hazelcast) {
    this.name = name;
    this.vertx = vertx;
    this.id = hazelcast.getIdGenerator(name);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void nextId(Handler<AsyncResult<Long>> resultHandler) {
    vertx.executeBlocking(new Action<Long>() {
      @Override
      public Long perform() {
        return id.newId();
      }
    }, resultHandler);
  }

}
