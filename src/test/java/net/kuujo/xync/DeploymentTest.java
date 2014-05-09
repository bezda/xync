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
package net.kuujo.xync;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;
import net.kuujo.xync.util.Cluster;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import org.vertx.testtools.TestVerticle;

/**
 * Deployment tests.
 *
 * @author Jordan Halterman
 */
public class DeploymentTest extends TestVerticle {

  public static class TestVerticle1 extends Verticle {
    @Override
    public void start() {
      System.out.println("Deploy test 1");
    }
  }

  public static class TestVerticle2 extends Verticle {
    @Override
    public void start() {
      System.out.println("Deploy test 2");
    }
  }

  public static class TestVerticle3 extends Verticle {
    @Override
    public void start() {
      System.out.println("Deploy test 3");
    }
  }

  @Test
  public void testDeployVerticlesToGroup() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("action", "deploy")
            .putString("id", "foo")
            .putString("type", "verticle")
            .putString("main", TestVerticle1.class.getName())
            .putBoolean("ha", true);
        vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
          @Override
          public void handle(Message<JsonObject> reply) {
            assertEquals("ok", reply.body().getString("status"));
            assertEquals("foo", reply.body().getString("id"));
            JsonObject message = new JsonObject()
                .putString("action", "deploy")
                .putString("id", "bar")
                .putString("type", "verticle")
                .putString("main", TestVerticle2.class.getName())
                .putBoolean("ha", true);
            vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
              @Override
              public void handle(Message<JsonObject> reply) {
                assertEquals("ok", reply.body().getString("status"));
                assertEquals("bar", reply.body().getString("id"));
                JsonObject message = new JsonObject()
                    .putString("action", "deploy")
                    .putString("id", "baz")
                    .putString("type", "verticle")
                    .putString("main", TestVerticle3.class.getName())
                    .putBoolean("ha", true);
                vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
                  @Override
                  public void handle(Message<JsonObject> reply) {
                    assertEquals("ok", reply.body().getString("status"));
                    assertEquals("baz", reply.body().getString("id"));
                    JsonObject message = new JsonObject()
                        .putString("action", "check")
                        .putString("id", "foo");
                    vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
                      @Override
                      public void handle(Message<JsonObject> reply) {
                        assertEquals("ok", reply.body().getString("status"));
                        assertTrue(reply.body().getBoolean("result"));
                        JsonObject message = new JsonObject()
                            .putString("action", "check")
                            .putString("id", "bar");
                        vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
                          @Override
                          public void handle(Message<JsonObject> reply) {
                            assertEquals("ok", reply.body().getString("status"));
                            assertTrue(reply.body().getBoolean("result"));
                            JsonObject message = new JsonObject()
                                .putString("action", "check")
                                .putString("id", "baz");
                            vertx.eventBus().send("test", message, new Handler<Message<JsonObject>>() {
                              @Override
                              public void handle(Message<JsonObject> reply) {
                                assertEquals("ok", reply.body().getString("status"));
                                assertTrue(reply.body().getBoolean("result"));
                                testComplete();
                              }
                            });
                          }
                        });
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
    });
  }

}
