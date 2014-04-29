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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.kuujo.xync.cluster.ClusterManager;
import net.kuujo.xync.cluster.ClusterManagerFactory;
import net.kuujo.xync.cluster.impl.HazelcastClusterManagerFactory;
import net.kuujo.xync.platform.PlatformManager;
import net.kuujo.xync.platform.PlatformManagerFactory;
import net.kuujo.xync.platform.impl.DefaultPlatformManagerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Future;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.Verticle;

/**
 * Xync cluster manager verticle.<p>
 *
 * This verticle supports a few options for managing and accessing the
 * Xync cluster. <code>cluster</code> indicates the cluster address at
 * which the cluster can be accessed. <code>group</code> indicates the
 * deployment group to which this verticle belongs. And <code>address</code>
 * indicates the absolute event bus address of this specific verticle.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Xync extends Verticle {
  private static final String DEFAULT_CLUSTER_ADDRESS = "cluster";
  private static final String DEFAULT_GROUP = "__DEFAULT__";
  private String cluster;
  private String group;
  private String address;
  private ClusterManager manager;
  private PlatformManager platform;

  private final Handler<Message<JsonObject>> clusterHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "info":
            doClusterInfo(message);
            break;
          case "check":
            doClusterCheck(message);
            break;
          case "deploy":
            doClusterDeploy(message);
            break;
          case "undeploy":
            doClusterUndeploy(message);
            break;
          default:
            String type = message.body().getString("type");
            if (type == null) {
              message.reply(new JsonObject().putString("status", "error").putString("message", "No data type specified."));
              return;
            }

            switch (type) {
              case "key":
                switch (action) {
                  case "get":
                    doKeyGet(message);
                    break;
                  case "set":
                    doKeySet(message);
                    break;
                  case "delete":
                    doKeyDelete(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
              case "counter":
                switch (action) {
                  case "increment":
                    doCounterIncrement(message);
                    break;
                  case "decrement":
                    doCounterDecrement(message);
                    break;
                  case "get":
                    doCounterGet(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
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
            }
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  private final Handler<Message<JsonObject>> groupHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "check":
            doCheck(message);
            break;
          case "deploy":
            doInternalDeploy(message);
            break;
          case "undeploy":
            doClusterUndeploy(message);
            break;
        }
      }
    }
  };

  private final Handler<Message<JsonObject>> internalHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(Message<JsonObject> message) {
      String action = message.body().getString("action");
      if (action != null) {
        switch (action) {
          case "info":
            doInfo(message);
            break;
          case "check":
            doCheck(message);
            break;
          case "deploy":
            doInternalDeploy(message);
            break;
          case "undeploy":
            doInternalUndeploy(message);
            break;
          default:
            String type = message.body().getString("type");
            if (type == null) {
              message.reply(new JsonObject().putString("status", "error").putString("message", "No data type specified."));
              return;
            }

            switch (type) {
              case "key":
                switch (action) {
                  case "get":
                    doKeyGet(message);
                    break;
                  case "set":
                    doKeySet(message);
                    break;
                  case "delete":
                    doKeyDelete(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
              case "counter":
                switch (action) {
                  case "increment":
                    doCounterIncrement(message);
                    break;
                  case "decrement":
                    doCounterDecrement(message);
                    break;
                  case "get":
                    doCounterGet(message);
                    break;
                  default:
                    message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
                    break;
                }
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
            }
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  private final Handler<Message<Object>> nodeHandler = new Handler<Message<Object>>() {
    @Override
    public void handle(Message<Object> message) {
      message.reply(address);
    }
  };

  @Override
  public void start(final Future<Void> future) {
    cluster = container.config().getString("cluster", DEFAULT_CLUSTER_ADDRESS);
    group = container.config().getString("group", DEFAULT_GROUP);
    address = container.config().getString("address", String.format("node-%s", UUID.randomUUID().toString()));

    ClassLoader loader = Thread.currentThread().getContextClassLoader();

    String sClusterFactory = container.config().getString("clusterFactory", HazelcastClusterManagerFactory.class.getName());
    try {
      Class<?> clazz = loader.loadClass(sClusterFactory);
      ClusterManagerFactory factory = (ClusterManagerFactory) clazz.newInstance();
      manager = (ClusterManager) clazz.getMethod("createClusterManager", new Class<?>[]{}).invoke(factory);
    } catch (Exception e) {
      future.setFailure(e);
      return;
    }

    String sPlatformFactory = container.config().getString("platformFactory", DefaultPlatformManagerFactory.class.getName());
    try {
      Class<?> clazz = loader.loadClass(sPlatformFactory);
      PlatformManagerFactory factory = (PlatformManagerFactory) clazz.newInstance();
      platform = (PlatformManager) clazz.getMethod("createPlatformManager", new Class<?>[]{Vertx.class, Container.class, ClusterManager.class, int.class, String.class, String.class, String.class})
          .invoke(factory, vertx, container, manager, container.config().getInteger("quorum", 1), cluster, group, address);
    } catch (Exception e) {
      future.setFailure(e);
      return;
    }

    platform.start();

    vertx.eventBus().registerHandler(address, internalHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          future.setFailure(result.cause());
        } else {
          vertx.eventBus().registerHandler(group, groupHandler, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                future.setFailure(result.cause());
              } else {
                vertx.eventBus().registerHandler(cluster, clusterHandler, new Handler<AsyncResult<Void>>() {
                  @Override
                  public void handle(AsyncResult<Void> result) {
                    if (result.failed()) {
                      future.setFailure(result.cause());
                    } else {
                      vertx.eventBus().registerHandler(String.format("%s.%s", cluster, manager.getNodeId()), nodeHandler, new Handler<AsyncResult<Void>>() {
                        @Override
                        public void handle(AsyncResult<Void> result) {
                          if (result.failed()) {
                            future.setFailure(result.cause());
                          } else {
                            Xync.super.start(future);
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

  @Override
  public void stop() {
    platform.stop();
  }

  /**
   * Gets deployment info for a deployment.
   */
  private void doClusterInfo(final Message<JsonObject> message) {
    doInfo(message);
  }

  /**
   * Checks whether a deployment is deployed.
   */
  private void doClusterCheck(final Message<JsonObject> message) {
    doCheck(message);
  }

  /**
   * Deploys a deployment.
   */
  private void doClusterDeploy(final Message<JsonObject> message) {
    String group = message.body().getString("group");
    if (group == null) {
      doInternalDeploy(message);
    } else {
      vertx.eventBus().sendWithTimeout(group, message.body(), 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
        @Override
        public void handle(AsyncResult<Message<JsonObject>> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(result.result());
          }
        }
      });
    }
  }

  /**
   * Undeploys a deployment.
   */
  private void doClusterUndeploy(final Message<JsonObject> message) {
    String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
      return;
    }

    platform.getAssignment(deploymentID, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          vertx.eventBus().sendWithTimeout(result.result(), message.body(), 30000, new Handler<AsyncResult<Message<JsonObject>>>() {
            @Override
            public void handle(AsyncResult<Message<JsonObject>> result) {
              if (result.failed()) {
                message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
              } else {
                message.reply(result.result());
              }
            }
          });
        }
      }
    });
  }

  /**
   * Gets info for a deployment.
   */
  private void doInfo(final Message<JsonObject> message) {
    String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
      return;
    }

    platform.getDeploymentInfo(deploymentID, new Handler<AsyncResult<JsonObject>>() {
      @Override
      public void handle(AsyncResult<JsonObject> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putObject("result", result.result()));
        }
      }
    });
  }

  /**
   * Checks whether a deployment is deployed.
   */
  private void doCheck(final Message<JsonObject> message) {
    String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
      return;
    }

    platform.isDeployed(deploymentID, new Handler<AsyncResult<Boolean>>() {
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
   * Deploys a deployment.
   */
  private void doInternalDeploy(final Message<JsonObject> message) {
    String type = message.body().getString("type");
    switch (type) {
      case "module":
        doDeployModule(message);
        break;
      case "verticle":
        doDeployVerticle(message);
        break;
    }
  }

  /**
   * Handles deployment of a module.
   */
  private void doDeployModule(final Message<JsonObject> message) {
    final String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No deployment ID specified."));
      return;
    }

    String module = message.body().getString("module");
    if (module == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No module name specified."));
      return;
    }

    JsonObject config = message.body().getObject("config");
    int instances = message.body().getInteger("instances", 1);
    boolean ha = message.body().getBoolean("ha", false);

    platform.deployModuleAs(deploymentID, module, config, instances, ha, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
        } else {
          message.reply(new JsonObject().putString("status", "ok").putString("id", result.result()));
        }
      }
    });
  }

  /**
   * Handles deployment of a verticle.
   */
  private void doDeployVerticle(final Message<JsonObject> message) {
    final String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No deployment ID specified."));
      return;
    }

    String main = message.body().getString("main");
    if (main == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No verticle main specified."));
      return;
    }

    JsonObject config = message.body().getObject("config");
    int instances = message.body().getInteger("instances", 1);
    boolean worker = message.body().getBoolean("worker", false);
    boolean ha = message.body().getBoolean("ha", false);

    if (worker) {
      boolean multiThreaded = message.body().getBoolean("multi-threaded", false);
      platform.deployWorkerVerticleAs(deploymentID, main, config, instances, multiThreaded, ha, new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(new JsonObject().putString("status", "ok").putString("id", result.result()));
          }
        }
      });
    } else {
      platform.deployVerticleAs(deploymentID, main, config, instances, ha, new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(new JsonObject().putString("status", "ok").putString("id", result.result()));
          }
        }
      });
    }
  }

  /**
   * Undeploys a deployment.
   */
  private void doInternalUndeploy(final Message<JsonObject> message) {
    String type = message.body().getString("type");
    if (type == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
      return;
    }

    String deploymentID = message.body().getString("id");
    if (deploymentID == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment ID."));
      return;
    }

    if (type.equals("module")) {
      platform.undeployModuleAs(deploymentID, new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(new JsonObject().putString("status", "ok"));
          }
        }
      });
    } else if (type.equals("verticle")) {
      platform.undeployVerticleAs(deploymentID, new Handler<AsyncResult<Void>>() {
        @Override
        public void handle(AsyncResult<Void> result) {
          if (result.failed()) {
            message.reply(new JsonObject().putString("status", "error").putString("message", result.cause().getMessage()));
          } else {
            message.reply(new JsonObject().putString("status", "ok"));
          }
        }
      });
    } else {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
    }
  }

  /**
   * Handles setting a key.
   */
  private void doKeySet(final Message<JsonObject> message) {
    final String key = message.body().getString("name");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    final Object value = message.body().getValue("value");

    try {
      manager.getMap(String.format("%s.keys", cluster)).put(key, value);
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  /**
   * Handles getting a key.
   */
  private void doKeyGet(final Message<JsonObject> message) {
    final String key = message.body().getString("name");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    try {
      Object value = manager.getMap(String.format("%s.keys", cluster)).get(key);
      message.reply(new JsonObject().putString("status", "ok").putValue("result", value));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  /**
   * Handles deleting a key.
   */
  private void doKeyDelete(final Message<JsonObject> message) {
    final String key = message.body().getString("name");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    try {
      manager.getMap(String.format("%s.keys", cluster)).remove(key);
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  /**
   * Handles getting a counter.
   */
  private void doCounterGet(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    try {
      Map<Object, Long> counters = manager.getMap(String.format("%s.counters", cluster));
      Long value = counters.get(name);
      if (value == null) {
        value = 0L;
      }
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", value));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  /**
   * Handles incrementing a counter.
   */
  private void doCounterIncrement(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    try {
      Map<Object, Long> counters = manager.getMap(String.format("%s.counters", cluster));
      Long value = counters.get(name);
      if (value == null) {
        value = 0L;
      }
      value++;
      counters.put(name, value);
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", value));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

  /**
   * Handles decrementing a counter.
   */
  private void doCounterDecrement(final Message<JsonObject> message) {
    final String name = message.body().getString("name");
    if (name == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No name specified."));
      return;
    }

    try {
      Map<Object, Long> counters = manager.getMap(String.format("%s.counters", cluster));
      Long value = counters.get(name);
      if (value == null) {
        value = 0L;
      }
      value--;
      counters.put(name, value);
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", value));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    final Object key = message.body().getValue("key");
    if (key == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No key specified."));
      return;
    }

    final Object value = message.body().getValue("value");
    if (value == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      return;
    }

    try {
      Object result = manager.getMap(String.format("%s.%s", cluster, name)).put(key, value);
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Object result = manager.getMap(String.format("%s.%s", cluster, name)).get(key);
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Object result = manager.getMap(String.format("%s.%s", cluster, name)).remove(key);
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getMap(String.format("%s.%s", cluster, name)).containsKey(key);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Set<Object> result = manager.getMap(String.format("%s.%s", cluster, name)).keySet();
      message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.toArray())));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Collection<Object> result = manager.getMap(String.format("%s.%s", cluster, name)).values();
      message.reply(new JsonObject().putString("status", "ok").putArray("result", new JsonArray(result.toArray())));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getMap(String.format("%s.%s", cluster, name)).isEmpty();
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      int result = manager.getMap(String.format("%s.%s", cluster, name)).size();
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      manager.getMap(String.format("%s.%s", cluster, name)).clear();
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getList(String.format("%s.%s", cluster, name)).add(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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
      try {
        Object result = manager.getList(String.format("%s.%s", cluster, name)).remove(index);
        message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
      } catch (Exception e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
      }
    }
    else {
      final Object value = message.body().getValue("value");
      if (value == null) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "No value specified."));
      }
      else {
        try {
          boolean result = manager.getList(String.format("%s.%s", cluster, name)).remove(value);
          message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
        } catch (Exception e) {
          message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
        }
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

    try {
      boolean result = manager.getList(String.format("%s.%s", cluster, name)).contains(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getList(String.format("%s.%s", cluster, name)).isEmpty();
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      int result = manager.getList(String.format("%s.%s", cluster, name)).size();
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      manager.getList(String.format("%s.%s", cluster, name)).clear();
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getSet(String.format("%s.%s", cluster, name)).add(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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
      try {
        boolean result = manager.getSet(String.format("%s.%s", cluster, name)).remove(value);
        message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
      } catch (Exception e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
      }
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

    try {
      boolean result = manager.getSet(String.format("%s.%s", cluster, name)).contains(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getSet(String.format("%s.%s", cluster, name)).isEmpty();
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      int result = manager.getSet(String.format("%s.%s", cluster, name)).size();
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      manager.getSet(String.format("%s.%s", cluster, name)).clear();
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getQueue(String.format("%s.%s", cluster, name)).add(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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
      try {
        boolean result = manager.getQueue(String.format("%s.%s", cluster, name)).remove(value);
        message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
      } catch (Exception e) {
        message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
      }
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

    try {
      boolean result = manager.getQueue(String.format("%s.%s", cluster, name)).contains(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getQueue(String.format("%s.%s", cluster, name)).isEmpty();
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      int result = manager.getQueue(String.format("%s.%s", cluster, name)).size();
      message.reply(new JsonObject().putString("status", "ok").putNumber("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      manager.getQueue(String.format("%s.%s", cluster, name)).clear();
      message.reply(new JsonObject().putString("status", "ok"));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      boolean result = manager.getQueue(String.format("%s.%s", cluster, name)).offer(value);
      message.reply(new JsonObject().putString("status", "ok").putBoolean("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Object result = manager.getQueue(String.format("%s.%s", cluster, name)).element();
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Object result = manager.getQueue(String.format("%s.%s", cluster, name)).poll();
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
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

    try {
      Object result = manager.getQueue(String.format("%s.%s", cluster, name)).peek();
      message.reply(new JsonObject().putString("status", "ok").putValue("result", result));
    } catch (Exception e) {
      message.reply(new JsonObject().putString("status", "error").putString("message", e.getMessage()));
    }
  }

}
