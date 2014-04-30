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
import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;
import net.kuujo.xync.util.Cluster;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.testtools.TestVerticle;

/**
 * Map data tests.
 *
 * @author Jordan Halterman
 */
public class MapTest extends TestVerticle {

  @Test
  public void testMapPut() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-put")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testMapPutPut() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-put-put")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            JsonObject message = new JsonObject()
                .putString("type", "map")
                .putString("name", "test-map-put-put")
                .putString("action", "put")
                .putString("key", "foo")
                .putString("value", "baz");
            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                assertTrue(result.succeeded());
                assertEquals("ok", result.result().body().getString("status"));
                assertNotNull(result.result().body().getString("result"));
                assertEquals("bar", result.result().body().getString("result"));
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testMapGet() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-get")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            JsonObject message = new JsonObject()
                .putString("type", "map")
                .putString("name", "test-map-get")
                .putString("action", "get")
                .putString("key", "foo");
            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                assertTrue(result.succeeded());
                assertEquals("ok", result.result().body().getString("status"));
                assertNotNull(result.result().body().getString("result"));
                assertEquals("bar", result.result().body().getString("result"));
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testMapRemove() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-remove")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            JsonObject message = new JsonObject()
                .putString("type", "map")
                .putString("name", "test-map-remove")
                .putString("action", "remove")
                .putString("key", "foo");
            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                assertTrue(result.succeeded());
                assertEquals("ok", result.result().body().getString("status"));
                assertNotNull(result.result().body().getString("result"));
                assertEquals("bar", result.result().body().getString("result"));
                JsonObject message = new JsonObject()
                    .putString("type", "map")
                    .putString("name", "test-map-remove")
                    .putString("action", "get")
                    .putString("key", "foo");
                vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                  @Override
                  public void handle(AsyncResult<Message<JsonObject>> result) {
                    assertTrue(result.succeeded());
                    assertEquals("ok", result.result().body().getString("status"));
                    assertNull(result.result().body().getString("result"));
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

  @Test
  public void testMapSize() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-size")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            JsonObject message = new JsonObject()
                .putString("type", "map")
                .putString("name", "test-map-size")
                .putString("action", "put")
                .putString("key", "bar")
                .putString("value", "baz");
            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                assertTrue(result.succeeded());
                assertEquals("ok", result.result().body().getString("status"));
                assertNull(result.result().body().getString("result"));
                JsonObject message = new JsonObject()
                    .putString("type", "map")
                    .putString("name", "test-map-size")
                    .putString("action", "put")
                    .putString("key", "baz")
                    .putString("value", "foo");
                vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                  @Override
                  public void handle(AsyncResult<Message<JsonObject>> result) {
                    assertTrue(result.succeeded());
                    assertEquals("ok", result.result().body().getString("status"));
                    assertNull(result.result().body().getString("result"));
                    JsonObject message = new JsonObject()
                        .putString("type", "map")
                        .putString("name", "test-map-size")
                        .putString("action", "size");
                    vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                      @Override
                      public void handle(AsyncResult<Message<JsonObject>> result) {
                        assertTrue(result.succeeded());
                        assertEquals("ok", result.result().body().getString("status"));
                        assertTrue(result.result().body().getInteger("result") == 3);
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

  @Test
  public void testMapClear() {
    Cluster.initialize();
    container.deployWorkerVerticle(Xync.class.getName(), new JsonObject().putString("cluster", "test"), 3, false, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        JsonObject message = new JsonObject()
            .putString("type", "map")
            .putString("name", "test-map-clear")
            .putString("action", "put")
            .putString("key", "foo")
            .putString("value", "bar");
        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            assertTrue(result.succeeded());
            assertEquals("ok", result.result().body().getString("status"));
            assertNull(result.result().body().getString("result"));
            JsonObject message = new JsonObject()
                .putString("type", "map")
                .putString("name", "test-map-clear")
                .putString("action", "put")
                .putString("key", "bar")
                .putString("value", "baz");
            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
              @Override
              public void handle(AsyncResult<Message<JsonObject>> result) {
                assertTrue(result.succeeded());
                assertEquals("ok", result.result().body().getString("status"));
                assertNull(result.result().body().getString("result"));
                JsonObject message = new JsonObject()
                    .putString("type", "map")
                    .putString("name", "test-map-clear")
                    .putString("action", "put")
                    .putString("key", "baz")
                    .putString("value", "foo");
                vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                  @Override
                  public void handle(AsyncResult<Message<JsonObject>> result) {
                    assertTrue(result.succeeded());
                    assertEquals("ok", result.result().body().getString("status"));
                    assertNull(result.result().body().getString("result"));
                    JsonObject message = new JsonObject()
                        .putString("type", "map")
                        .putString("name", "test-map-clear")
                        .putString("action", "size");
                    vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                      @Override
                      public void handle(AsyncResult<Message<JsonObject>> result) {
                        assertTrue(result.succeeded());
                        assertEquals("ok", result.result().body().getString("status"));
                        assertTrue(result.result().body().getInteger("result") == 3);
                        JsonObject message = new JsonObject()
                            .putString("type", "map")
                            .putString("name", "test-map-clear")
                            .putString("action", "clear");
                        vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                          @Override
                          public void handle(AsyncResult<Message<JsonObject>> result) {
                            assertTrue(result.succeeded());
                            assertEquals("ok", result.result().body().getString("status"));
                            assertNull(result.result().body().getString("result"));
                            JsonObject message = new JsonObject()
                                .putString("type", "map")
                                .putString("name", "test-map-clear")
                                .putString("action", "size");
                            vertx.eventBus().sendWithTimeout("test", message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
                              @Override
                              public void handle(AsyncResult<Message<JsonObject>> result) {
                                assertTrue(result.succeeded());
                                assertEquals("ok", result.result().body().getString("status"));
                                assertTrue(result.result().body().getInteger("result") == 0);
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
