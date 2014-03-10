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
package net.kuujo.zync.cluster.impl;

import java.util.Map;
import java.util.Set;

import net.kuujo.zync.cluster.DeploymentInfo;
import net.kuujo.zync.cluster.Event;
import net.kuujo.zync.cluster.ModuleDeploymentInfo;
import net.kuujo.zync.cluster.ZyncClusterManager;
import net.kuujo.zync.cluster.ZyncClusterService;
import net.kuujo.zync.cluster.impl.DefaultModuleDeploymentInfo;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.Action;

/**
 * Default cluster service implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultZyncClusterService implements ZyncClusterService {
  private final String CLUSTER_ADDRESS = "zync";
  private static final String DEPLOYMENTS_MAP_NAME = "__zync.deployments";

  private final VertxInternal vertx;
  private final EventBus eventBus;
  private final Map<String, String> deployments;
  private final ZyncClusterManager clusterManager;

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "deploy":
            doDeploy(message);
            break;
          case "undeploy":
            doUndeploy(message);
            break;
          case "set":
            doSet(message);
            break;
          case "get":
            doGet(message);
            break;
          case "delete":
            doDelete(message);
            break;
          case "exists":
            doExists(message);
            break;
          case "keys":
            doKeys(message);
            break;
          case "watch":
            doWatch(message);
            break;
          case "unwatch":
            doUnwatch(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public DefaultZyncClusterService(ZyncClusterManager clusterManager) {
    this.vertx = clusterManager.vertx();
    this.eventBus = vertx.eventBus();
    this.deployments = clusterManager.cluster().getSyncMap(DEPLOYMENTS_MAP_NAME);
    this.clusterManager = clusterManager;
  }

  @Override
  public ZyncClusterService start(Handler<AsyncResult<Void>> doneHandler) {
    eventBus.registerHandler(CLUSTER_ADDRESS, messageHandler, doneHandler);
    return null;
  }

  @Override
  public void stop(Handler<AsyncResult<Void>> doneHandler) {
    eventBus.unregisterHandler(CLUSTER_ADDRESS, messageHandler, doneHandler);
  }

  /**
   * Handles deployment of a module/verticle.
   */
  private void doDeploy(final Message<JsonObject> message) {
    String stype = message.body().getString("type");
    if (stype == null) {
      // TODO support verticle deployments
      // message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
      // return;
      // For now we just support module deployments
      stype = "module";
    }

    DeploymentInfo.Type type = DeploymentInfo.Type.parse(stype);
    if (type.equals(DeploymentInfo.Type.MODULE)) {
      final ModuleDeploymentInfo deploymentInfo = new DefaultModuleDeploymentInfo(message.body());

      // If the deployment info indicates a deployment group then attempt to forward
      // the deployment to the appropriate group.
      String group = deploymentInfo.group();
      if (group == null) {
        group = "__DEFAULT__";
      }

      if (group.equals(clusterManager.group())) {
        clusterManager.deployModuleAs(deploymentInfo.id(), deploymentInfo.module(), deploymentInfo.config(), deploymentInfo.instances(), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putString("id", deploymentInfo.id()));
            }
          }
        });
      } else {
        eventBus.sendWithTimeout(group, message.body(), 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
          @Override
          public void handle(AsyncResult<Message<JsonObject>> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            } else {
              message.reply(result.result().body());
            }
          }
        });
      }
    } else {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Unsupported deployment type."));
    }
  }

  /**
   * Handles undeployment of a module/verticle.
   */
  private void doUndeploy(final Message<JsonObject> message) {
    final String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No deployment ID specified."));
      return;
    }

    // Perform a reverse lookup of the node on which the deployment is deployed.
    vertx.executeBlocking(new Action<String>() {
      @Override
      public String perform() {
        return deployments.get(deploymentID);
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else if (result.result() == null) {
          message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
        }
        else {
          String nodeID = result.result();
          eventBus.sendWithTimeout(nodeID, message.body(), 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
              } else {
                message.reply(result.result().body());
              }
            }
          });
        }
      }
    });
  }

  private void doSet(final Message<JsonObject> message) {
    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    final Object value = message.body().getString("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      return;
    }

    clusterManager.set(key, value, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  private void doGet(final Message<JsonObject> message) {
    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    final Object defaultValue = message.body().getString("default");

    clusterManager.get(key, defaultValue, new Handler<AsyncResult<Object>>() {
      @Override
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
        }
      }
    });
  }

  private void doDelete(final Message<JsonObject> message) {
    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    clusterManager.delete(key, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  private void doExists(final Message<JsonObject> message) {
    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    clusterManager.exists(key, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  private void doKeys(final Message<JsonObject> message) {
    clusterManager.keys(new Handler<AsyncResult<Set<String>>>() {
      @Override
      public void handle(AsyncResult<Set<String>> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.result().toArray(new String[result.result().size()]))));
        }
      }
    });
  }

  private void doWatch(final Message<JsonObject> message) {
    String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    String address = message.body().getString("address");
    if (address == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No address specified."));
      return;
    }

    String sevent = message.body().getString("event");
    Event.Type event = null;
    if (sevent != null) {
      try {
        event = Event.Type.parse(sevent);
      }
      catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.watch(address, key, event, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  private void doUnwatch(final Message<JsonObject> message) {
    String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    String address = message.body().getString("address");
    if (address == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No address specified."));
      return;
    }

    String sevent = message.body().getString("event");
    Event.Type event = null;
    if (sevent != null) {
      try {
        event = Event.Type.parse(sevent);
      }
      catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.unwatch(address, key, event, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        }
        else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

}
