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
package net.kuujo.xync.cluster.impl;

import java.util.Map;

import net.kuujo.xync.cluster.DeploymentInfo;
import net.kuujo.xync.cluster.Event;
import net.kuujo.xync.cluster.ModuleDeploymentInfo;
import net.kuujo.xync.cluster.VerticleDeploymentInfo;
import net.kuujo.xync.cluster.WorkerVerticleDeploymentInfo;
import net.kuujo.xync.cluster.XyncClusterManager;
import net.kuujo.xync.cluster.XyncClusterService;

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
public class DefaultXyncClusterService implements XyncClusterService {
  private static final String CLUSTER_ADDRESS = "__CLUSTER__";
  private static final String CLUSTER_MAP_NAME = "__xync.cluster";
  private static final String DEFAULT_GROUP = "__DEFAULT__";

  private final VertxInternal vertx;
  private final EventBus eventBus;
  private final Map<String, String> cluster;
  private final XyncClusterManager clusterManager;

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
          case "id":
            doGenerateId(message);
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
          case "watch":
            doWatch(message);
            break;
          case "unwatch":
            doUnwatch(message);
            break;
          case "add":
            doAdd(message);
            break;
          case "remove":
            doRemove(message);
            break;
          case "contains":
            doContains(message);
            break;
          case "count":
            doCount(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public DefaultXyncClusterService(XyncClusterManager clusterManager) {
    this.vertx = clusterManager.vertx();
    this.eventBus = vertx.eventBus();
    this.cluster = clusterManager.cluster().getSyncMap(CLUSTER_MAP_NAME);
    this.clusterManager = clusterManager;
  }

  @Override
  public XyncClusterService start(Handler<AsyncResult<Void>> doneHandler) {
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
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
      return;
    }

    // Parse the deployment type.
    DeploymentInfo.Type type;
    try {
      type = DeploymentInfo.Type.parse(stype);
    } catch (IllegalArgumentException e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Unsupported deployment type."));
      return;
    }

    // If the deployment info indicates a deployment group then attempt to forward
    // the deployment to the appropriate group.
    String group = message.body().getString("group");
    if (group == null) {
      group = DEFAULT_GROUP;
    }

    if (!group.equals(clusterManager.group())) {
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
    } else {
      if (type.equals(DeploymentInfo.Type.MODULE)) {
        final ModuleDeploymentInfo deploymentInfo = new DefaultModuleDeploymentInfo(message.body());
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
      } else if (type.equals(DeploymentInfo.Type.VERTICLE)) {
        boolean isWorker = message.body().getBoolean("worker", false);
        if (isWorker) {
          final WorkerVerticleDeploymentInfo deploymentInfo = new DefaultWorkerVerticleDeploymentInfo(message.body());
          clusterManager.deployWorkerVerticleAs(deploymentInfo.id(), deploymentInfo.main(), deploymentInfo.config(), deploymentInfo.classpath(), deploymentInfo.instances(), deploymentInfo.isMultiThreaded(), deploymentInfo.includes(), new Handler<AsyncResult<String>>() {
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
          final VerticleDeploymentInfo deploymentInfo = new DefaultVerticleDeploymentInfo(message.body());
          clusterManager.deployVerticleAs(deploymentInfo.id(), deploymentInfo.main(), deploymentInfo.config(), deploymentInfo.classpath(), deploymentInfo.instances(), deploymentInfo.includes(), new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> result) {
              if (result.failed()) {
                message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
              } else {
                message.reply(new JsonObject().putString("status", "ok").putString("id", deploymentInfo.id()));
              }
            }
          });
        }
      }
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
        for (Map.Entry<String, String> entry : DefaultXyncClusterService.this.cluster.entrySet()) {
          JsonObject info = new JsonObject(entry.getValue());
          JsonArray deployments = info.getArray("deployments");
          if (deployments != null) {
            for (Object deployment : deployments) {
              JsonObject deploymentInfo = (JsonObject) deployment;
              String id = deploymentInfo.getString("id");
              if (id != null && id.equals(deploymentID)) {
                return entry.getKey();
              }
            }
          }
        }
        return null;
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else if (result.result() == null) {
          message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
        } else {
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

  /**
   * Handles generating a cluster-wide unique ID.
   */
  private void doGenerateId(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.generateId(name, new Handler<AsyncResult<Long>>() {
      @Override
      public void handle(AsyncResult<Long> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putNumber("id", result.result()));
        }
      }
    });
  }

  /**
   * Handles a cluster set command.
   */
  private void doSet(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

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

    clusterManager.set(name, key, value, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Handles a cluster get command.
   */
  private void doGet(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    final Object defaultValue = message.body().getString("default");

    clusterManager.get(name, key, defaultValue, new Handler<AsyncResult<Object>>() {
      @Override
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
        }
      }
    });
  }

  /**
   * Handles a cluster delete command.
   */
  private void doDelete(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    clusterManager.delete(name, key, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Handles a cluster exists command.
   */
  private void doExists(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final String key = message.body().getString("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    clusterManager.exists(name, key, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Handles a cluster watch command.
   */
  private void doWatch(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

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
      } catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.watch(name, key, address, event, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Handles a cluster unwatch command.
   */
  private void doUnwatch(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

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
      } catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.unwatch(name, key, address, event, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Handles a list addition.
   */
  private void doAdd(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final Object value = message.body().getValue("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      return;
    }

    clusterManager.add(name, value, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok"));
        }
      }
    });
  }

  /**
   * Handles a list removal.
   */
  private void doRemove(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    if (message.body().containsField("index")) {
      final int index = message.body().getInteger("index");
      clusterManager.remove(name, index, new Handler<AsyncResult<Object>>() {
        @Override
        public void handle(AsyncResult<Object> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(new JsonObject().putString("status", "ok").putValue("result", result.result()));
          }
        }
      });
    }
    else {
      final Object value = message.body().getValue("value");
      if (value == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      }
      else {
        clusterManager.remove(name, value, new Handler<AsyncResult<Boolean>>() {
          @Override
          public void handle(AsyncResult<Boolean> result) {
            if (result.failed()) {
              message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
            }
          }
        });
      }
    }
  }

  /**
   * Checks whether a list contains a value.
   */
  private void doContains(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final Object value = message.body().getValue("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      return;
    }

    clusterManager.contains(name, value, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result.result()));
        }
      }
    });
  }

  /**
   * Counts the number of items in a list.
   */
  private void doCount(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.count(name, new Handler<AsyncResult<Integer>>() {
      @Override
      public void handle(AsyncResult<Integer> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putNumber("result", result.result()));
        }
      }
    });
  }

}
