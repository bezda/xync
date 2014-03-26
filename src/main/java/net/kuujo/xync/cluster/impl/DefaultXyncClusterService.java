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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import net.kuujo.xync.cluster.DeploymentInfo;
import net.kuujo.xync.cluster.ModuleDeploymentInfo;
import net.kuujo.xync.cluster.VerticleDeploymentInfo;
import net.kuujo.xync.cluster.WorkerVerticleDeploymentInfo;
import net.kuujo.xync.cluster.XyncClusterManager;
import net.kuujo.xync.cluster.XyncClusterService;
import net.kuujo.xync.cluster.data.MapEvent;

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
          default:
            String type = message.body().getString("type");
            if (type == null) {
              message.reply(new JsonObject().putString("status", "error").putString("message", "No data type specified."));
              return;
            }

            switch (type) {
              case "map":
                switch (action) {
                  case "put":
                    doMapPut(message);
                    break;
                  case "get":
                    doMapGet(message);
                    break;
                  case "remove":
                    doMapRemove(message);
                    break;
                  case "contains":
                    doMapContainsKey(message);
                    break;
                  case "keys":
                    doMapKeys(message);
                    break;
                  case "values":
                    doMapValues(message);
                    break;
                  case "empty":
                    doMapIsEmpty(message);
                    break;
                  case "clear":
                    doMapClear(message);
                    break;
                  case "size":
                    doMapSize(message);
                    break;
                  case "watch":
                    doMapWatch(message);
                    break;
                  case "unwatch":
                    doMapUnwatch(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
                break;
              case "list":
                switch (action) {
                  case "add":
                    doListAdd(message);
                    break;
                  case "remove":
                    doListRemove(message);
                    break;
                  case "contains":
                    doListContains(message);
                    break;
                  case "size":
                    doListSize(message);
                    break;
                  case "empty":
                    doListIsEmpty(message);
                    break;
                  case "clear":
                    doListClear(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
                break;
              case "set":
                switch (action) {
                  case "add":
                    doSetAdd(message);
                    break;
                  case "remove":
                    doSetRemove(message);
                    break;
                  case "contains":
                    doSetContains(message);
                    break;
                  case "size":
                    doSetSize(message);
                    break;
                  case "empty":
                    doSetIsEmpty(message);
                    break;
                  case "clear":
                    doSetClear(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
                break;
              case "queue":
                switch (action) {
                  case "add":
                    doQueueAdd(message);
                    break;
                  case "remove":
                    doQueueRemove(message);
                    break;
                  case "contains":
                    doQueueContains(message);
                    break;
                  case "empty":
                    doQueueIsEmpty(message);
                    break;
                  case "size":
                    doQueueSize(message);
                    break;
                  case "clear":
                    doQueueClear(message);
                    break;
                  case "offer":
                    doQueueOffer(message);
                    break;
                  case "element":
                    doQueueElement(message);
                    break;
                  case "poll":
                    doQueuePoll(message);
                    break;
                  case "peek":
                    doQueuePeek(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
                break;
              case "lock":
                switch (action) {
                  case "lock":
                    doLockLock(message);
                    break;
                  case "try":
                    doLockTry(message);
                    break;
                  case "unlock":
                    doLockUnlock(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
              case "id":
                switch (action) {
                  case "next":
                    doGenerateId(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
                break;
            }
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

    clusterManager.getIdGenerator(name).nextId(new Handler<AsyncResult<Long>>() {
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
   * Handles a cluster map put command.
   */
  private void doMapPut(final Message<JsonObject> message) {
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

    clusterManager.getMap(name).put(key, value, new Handler<AsyncResult<Object>>() {
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
   * Handles a cluster map get command.
   */
  private void doMapGet(final Message<JsonObject> message) {
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

    clusterManager.getMap(name).get(key, new Handler<AsyncResult<Object>>() {
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
   * Handles a cluster map remove command.
   */
  private void doMapRemove(final Message<JsonObject> message) {
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

    clusterManager.getMap(name).remove(key, new Handler<AsyncResult<Object>>() {
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
   * Handles a cluster exists command.
   */
  private void doMapContainsKey(final Message<JsonObject> message) {
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

    clusterManager.getMap(name).containsKey(key, new Handler<AsyncResult<Boolean>>() {
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
   * Handles map keys command.
   */
  private void doMapKeys(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getMap(name).keySet(new Handler<AsyncResult<Set<Object>>>() {
      @Override
      public void handle(AsyncResult<Set<Object>> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.result().toArray())));
        }
      }
    });
  }

  /**
   * Handles map values command.
   */
  private void doMapValues(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getMap(name).values(new Handler<AsyncResult<Collection<Object>>>() {
      @Override
      public void handle(AsyncResult<Collection<Object>> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.result().toArray())));
        }
      }
    });
  }

  /**
   * Handles cluster map is empty command.
   */
  private void doMapIsEmpty(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getMap(name).isEmpty(new Handler<AsyncResult<Boolean>>() {
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
   * Counts the number of items in a map.
   */
  private void doMapSize(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getMap(name).size(new Handler<AsyncResult<Integer>>() {
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

  /**
   * Clears all items in a map.
   */
  private void doMapClear(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getMap(name).clear(new Handler<AsyncResult<Void>>() {
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
   * Handles a cluster watch command.
   */
  private void doMapWatch(final Message<JsonObject> message) {
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
    MapEvent.Type event = null;
    if (sevent != null) {
      try {
        event = MapEvent.Type.parse(sevent);
      } catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.getMap(name).watch(key, event, address, new Handler<AsyncResult<Void>>() {
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
  private void doMapUnwatch(final Message<JsonObject> message) {
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
    MapEvent.Type event = null;
    if (sevent != null) {
      try {
        event = MapEvent.Type.parse(sevent);
      } catch (IllegalArgumentException e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        return;
      }
    }

    clusterManager.getMap(name).unwatch(key, event, address, new Handler<AsyncResult<Void>>() {
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
  private void doListAdd(final Message<JsonObject> message) {
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

    clusterManager.getList(name).add(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles a list removal.
   */
  private void doListRemove(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    if (message.body().containsField("index")) {
      final int index = message.body().getInteger("index");
      clusterManager.getList(name).remove(index, new Handler<AsyncResult<Object>>() {
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
        clusterManager.getList(name).remove(value, new Handler<AsyncResult<Boolean>>() {
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
  private void doListContains(final Message<JsonObject> message) {
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

    clusterManager.getList(name).contains(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles cluster list is empty command.
   */
  private void doListIsEmpty(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getList(name).isEmpty(new Handler<AsyncResult<Boolean>>() {
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
  private void doListSize(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getList(name).size(new Handler<AsyncResult<Integer>>() {
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

  /**
   * Clears all items in a list.
   */
  private void doListClear(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getList(name).clear(new Handler<AsyncResult<Void>>() {
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
   * Handles a set addition.
   */
  private void doSetAdd(final Message<JsonObject> message) {
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

    clusterManager.getSet(name).add(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles a set removal.
   */
  private void doSetRemove(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final Object value = message.body().getValue("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
    }
    else {
      clusterManager.getSet(name).remove(value, new Handler<AsyncResult<Boolean>>() {
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

  /**
   * Checks whether a set contains a value.
   */
  private void doSetContains(final Message<JsonObject> message) {
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

    clusterManager.getSet(name).contains(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles cluster set is empty command.
   */
  private void doSetIsEmpty(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getSet(name).isEmpty(new Handler<AsyncResult<Boolean>>() {
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
   * Counts the number of items in a set.
   */
  private void doSetSize(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getSet(name).size(new Handler<AsyncResult<Integer>>() {
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

  /**
   * Clears all items in a set.
   */
  private void doSetClear(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getSet(name).clear(new Handler<AsyncResult<Void>>() {
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
   * Handles a queue addition.
   */
  private void doQueueAdd(final Message<JsonObject> message) {
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

    clusterManager.getQueue(name).add(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles a queue removal.
   */
  private void doQueueRemove(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final Object value = message.body().getValue("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
    }
    else {
      clusterManager.getQueue(name).remove(value, new Handler<AsyncResult<Boolean>>() {
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

  /**
   * Checks whether a queue contains a value.
   */
  private void doQueueContains(final Message<JsonObject> message) {
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

    clusterManager.getQueue(name).contains(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles cluster queue is empty command.
   */
  private void doQueueIsEmpty(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).isEmpty(new Handler<AsyncResult<Boolean>>() {
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
   * Counts the number of items in a queue.
   */
  private void doQueueSize(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).size(new Handler<AsyncResult<Integer>>() {
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

  /**
   * Clears all items in a queue.
   */
  private void doQueueClear(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).clear(new Handler<AsyncResult<Void>>() {
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
   * Handles a queue offer command.
   */
  private void doQueueOffer(final Message<JsonObject> message) {
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

    clusterManager.getQueue(name).offer(value, new Handler<AsyncResult<Boolean>>() {
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
   * Handles a queue element command.
   */
  private void doQueueElement(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).element(new Handler<AsyncResult<Object>>() {
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
   * Handles a queue poll command.
   */
  private void doQueuePoll(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).poll(new Handler<AsyncResult<Object>>() {
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
   * Handles a queue peek command.
   */
  private void doQueuePeek(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getQueue(name).peek(new Handler<AsyncResult<Object>>() {
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
   * Locks a lock.
   */
  private void doLockLock(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getLock(name).lock(new Handler<AsyncResult<Void>>() {
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
   * Tries to lock a lock.
   */
  private void doLockTry(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    final long timeout = message.body().getLong("timeout", 30000);

    clusterManager.getLock(name).tryLock(timeout, new Handler<AsyncResult<Boolean>>() {
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
   * Unlocks a lock.
   */
  private void doLockUnlock(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    clusterManager.getLock(name).unlock(new Handler<AsyncResult<Void>>() {
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

}
