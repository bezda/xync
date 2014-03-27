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

import java.util.UUID;

import net.kuujo.xync.cluster.data.MapEvent;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Watch example.
 *
 * @author Jordan Halterman
 */
public class WatchExample extends Verticle {

  @Override
  public void start() {
    // Register an event bus handler at which to receive event messages.
    final String address = UUID.randomUUID().toString();
    vertx.eventBus().registerHandler(address, new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> message) {
        String event = message.body().getString("event");
        String key = message.body().getString("key");
        Object value = message.body().getValue("value");
        container.logger().info("Received " + event + " event for key " + key + " with value " + value);
      }
    }, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          container.logger().error(result.cause());
        }
        else {
          // Once the handler has been registered, watch a key.
          JsonObject message = new JsonObject()
              .putString("action", "watch")
              .putString("key", "test")
              .putString("event", MapEvent.Type.CHANGE.toString());
          vertx.eventBus().sendWithTimeout("cluster", message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                container.logger().error(result.cause());
              }
              else if (result.result().body().getString("status").equals("error")) {
                container.logger().error(result.result().body().getString("message"));
              }
              else {
                // Now set the key that's being watched. This will trigger both the
                // create and change events.
                JsonObject message = new JsonObject()
                    .putString("action", "set")
                    .putString("key", "test")
                    .putString("value", "Hello world!");
                vertx.eventBus().sendWithTimeout("cluster", message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
                  @Override
                  public void handle(AsyncResult<Message<JsonObject>> result) {
                    if (result.failed()) {
                      container.logger().error(result.cause());
                    }
                    else if (result.result().body().getString("status").equals("ok")) {
    
                      // Get the value from the cluster. Reading a value never triggers
                      // any event since the data is not changed. Default values are returned
                      // to the requester but not applied to the internal data.
                      JsonObject message = new JsonObject()
                          .putString("action", "get")
                          .putString("key", "test");
                      vertx.eventBus().sendWithTimeout("cluster", message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
                        @Override
                        public void handle(AsyncResult<Message<JsonObject>> result) {
                          if (result.failed()) {
                            container.logger().error(result.cause());
                          }
                          else if (result.result().body().getString("status").equals("ok")) {
                            container.logger().info("Value of test is " + result.result().body().getString("result"));
    
                            // Delete the key from the cluster. Deleting a key will trigger
                            // a delete and change event.
                            JsonObject message = new JsonObject()
                                .putString("action", "delete")
                                .putString("key", "test");
                            vertx.eventBus().sendWithTimeout("cluster", message, 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
                              @Override
                              public void handle(AsyncResult<Message<JsonObject>> result) {
                                if (result.failed()) {
                                  container.logger().error(result.cause());
                                }
                                else if (result.result().body().getString("status").equals("ok")) {
                                  container.logger().info("Deleted key test");
                                }
                              }
                            });
                          }
                        }
                      });
                    }
                  }
                });
              }
            }
          });
        }
      }
    });
  }

}
