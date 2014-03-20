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
package net.kuujo.xync.test.integration.service;

import static org.vertx.testtools.VertxAssert.assertEquals;
import static org.vertx.testtools.VertxAssert.assertNull;
import static org.vertx.testtools.VertxAssert.assertTrue;
import static org.vertx.testtools.VertxAssert.testComplete;

import java.util.Set;
import java.util.UUID;

import net.kuujo.xync.cluster.Event;
import net.kuujo.xync.cluster.XyncClusterClient;
import net.kuujo.xync.cluster.impl.DefaultXyncClusterClient;
import net.kuujo.xync.test.integration.XyncTestVerticle;

import org.junit.Test;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * A network auditor test.
 *
 * @author Jordan Halterman
 */
public class ServiceTest extends XyncTestVerticle {

  @Test
  public void testDeployModule() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.deployModule("test1", "net.kuujo~xync-test1~1.0", new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        assertEquals("test1", result.result());
        testComplete();
      }
    });
  }

  @Test
  public void testDeployModuleToGroup() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.deployModuleTo("test1", "__DEFAULT__", "net.kuujo~xync-test1~1.0", new JsonObject(), 1, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        assertEquals("test1", result.result());
        testComplete();
      }
    });
  }

  @Test
  public void testUndeployModule() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.deployModule("test1", "net.kuujo~xync-test1~1.0", new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        assertEquals("test1", result.result());
        client.undeployModule("test1", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testUndeployModuleToGroup() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.deployModuleTo("test1", "__DEFAULT__", "net.kuujo~xync-test1~1.0", new JsonObject(), 1, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        assertEquals("test1", result.result());
        client.undeployModule("test1", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testMissingGet() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.get("test", null, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        assertTrue(result.succeeded());
        assertNull(result.result());
        testComplete();
      }
    });
  }

  @Test
  public void testSetGet() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.get("test", null, new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            assertTrue(result.succeeded());
            assertEquals("Hello world!", result.result());
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testDelete() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.delete("test", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            client.get("test", null, new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                assertTrue(result.succeeded());
                assertNull(result.result());
                testComplete();
              }
            });
          }
        });
      }
    });
  }

  @Test
  public void testExists() {
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.set("test", "Hello world!", new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.exists("test", new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            assertTrue(result.succeeded());
            assertTrue(result.result());
            testComplete();
          }
        });
      }
    });
  }

  @Test
  public void testWatchCreate() {
    final String key = UUID.randomUUID().toString();
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.watch(key, Event.Type.CREATE, new Handler<Event>() {
      @Override
      public void handle(Event event) {
        assertEquals(Event.Type.CREATE, event.type());
        assertEquals(key, event.key());
        assertEquals("Hello world!", event.value());
        testComplete();
      }
    }, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.set(key, "Hello world!");
      }
    });
  }

  @Test
  public void testWatchUpdate() {
    final String key = UUID.randomUUID().toString();
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.watch(key, Event.Type.UPDATE, new Handler<Event>() {
      @Override
      public void handle(Event event) {
        assertEquals(Event.Type.UPDATE, event.type());
        assertEquals(key, event.key());
        assertEquals("Hello world again!", event.value());
        testComplete();
      }
    }, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.set(key, "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            client.set(key, "Hello world again!");
          }
        });
      }
    });
  }

  @Test
  public void testWatchDelete() {
    final String key = UUID.randomUUID().toString();
    final XyncClusterClient client = new DefaultXyncClusterClient(vertx.eventBus());
    client.watch(key, Event.Type.DELETE, new Handler<Event>() {
      @Override
      public void handle(Event event) {
        assertEquals(Event.Type.DELETE, event.type());
        assertEquals(key, event.key());
        assertEquals("Hello world!", event.value());
        testComplete();
      }
    }, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        assertTrue(result.succeeded());
        client.set(key, "Hello world!", new Handler<AsyncResult<Void>>() {
          @Override
          public void handle(AsyncResult<Void> result) {
            assertTrue(result.succeeded());
            client.delete(key);
          }
        });
      }
    });
  }

}
