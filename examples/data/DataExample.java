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

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

/**
 * Cluster data example.
 *
 * @author Jordan Halterman
 */
public class DataExample extends Verticle {

  @Override
  public void start() {
    // Set a key in the cluster.
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

          // Get the value from the cluster.
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

                // Delete the key from the cluster.
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
