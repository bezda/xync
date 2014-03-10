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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.kuujo.zync.cluster.DeploymentInfo;
import net.kuujo.zync.cluster.Event;
import net.kuujo.zync.cluster.ModuleDeploymentInfo;
import net.kuujo.zync.cluster.ZyncClusterManager;
import net.kuujo.zync.cluster.Event.Type;
import net.kuujo.zync.cluster.impl.ZyncAsyncMap;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.Action;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.platform.impl.PlatformManagerInternal;

/**
 * Default cluster manager implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultZyncClusterManager implements ZyncClusterManager {
  private static final String DATA_MAP_NAME = "__zync.data";
  private static final String WATCHERS_MAP_NAME = "__zync.watchers";
  private static final String DEPLOYMENTS_MAP_NAME = "__zync.deployments";
  private static final String DEPLOYMENT_IDS_MAP_NAME = "__zync.deploymentIDs";
  private static final String USER_IDS_MAP_NAME = "__zync.userIDs";

  private final String nodeID;
  private final String group;
  private final VertxInternal vertx;
  private final PlatformManagerInternal platform;
  private final EventBus eventBus;
  private final ClusterManager clusterManager;
  private final Map<String, String> deployments;
  private final Map<String, String> deploymentIDs;
  private final Map<String, String> userIDs;
  private final ZyncAsyncMap<String, Object> data;
  private final ZyncAsyncMap<String, String> watchers;

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
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
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
          case "undeploy":
            doInternalUndeploy(message);
            break;
          default:
            message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid action " + action));
            break;
        }
      }
    }
  };

  public DefaultZyncClusterManager(String group, VertxInternal vertx, PlatformManagerInternal platform, ClusterManager clusterManager) {
    this.nodeID = clusterManager.getNodeID();
    this.group = group != null ? group : "__DEFAULT__";
    this.vertx = vertx;
    this.platform = platform;
    this.eventBus = vertx.eventBus();
    this.clusterManager = clusterManager;
    this.deployments = clusterManager.getSyncMap(DEPLOYMENTS_MAP_NAME);
    this.deploymentIDs = clusterManager.getSyncMap(DEPLOYMENT_IDS_MAP_NAME);
    this.userIDs = clusterManager.getSyncMap(USER_IDS_MAP_NAME);
    this.data = new ZyncAsyncMap<String, Object>(vertx, clusterManager.<String, Object>getSyncMap(DATA_MAP_NAME));
    this.watchers = new ZyncAsyncMap<String, String>(vertx, clusterManager.<String, String>getSyncMap(WATCHERS_MAP_NAME));
  }

  @Override
  public String address() {
    return nodeID;
  }

  @Override
  public String group() {
    return group;
  }

  @Override
  public VertxInternal vertx() {
    return vertx;
  }

  @Override
  public PlatformManagerInternal platform() {
    return platform;
  }

  @Override
  public ClusterManager cluster() {
    return clusterManager;
  }

  @Override
  public ZyncClusterManager start(final Handler<AsyncResult<Void>> doneHandler) {
    eventBus.registerHandler(nodeID, internalHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          eventBus.registerHandler(group, messageHandler, doneHandler);
        }
      }
    });
    return this;
  }

  @Override
  public void stop(final Handler<AsyncResult<Void>> doneHandler) {
    eventBus.unregisterHandler(group, messageHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          eventBus.unregisterHandler(nodeID, internalHandler);
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        }
        else {
          eventBus.unregisterHandler(nodeID, internalHandler, doneHandler);
        }
      }
    });
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

      // If the deployment info indicates a group other than the current cluster
      // group then return an error.
      String group = deploymentInfo.group();
      if (group == null) {
        group = "__DEFAULT__";
      }

      if (!group.equals(this.group)) {
        message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment group."));
      } else {
        deployModuleAs(deploymentInfo.id(), deploymentInfo.module(), deploymentInfo.config(), deploymentInfo.instances(), new Handler<AsyncResult<String>>() {
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

  /**
   * Handles undeployment of a module/verticle.
   */
  private void doInternalUndeploy(final Message<JsonObject> message) {
    final String id = message.body().getString("id");
    if (id == null) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "No deployment ID specified."));
      return;
    }

    undeployModuleAs(id, new Handler<AsyncResult<Void>>() {
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

  @Override
  public ZyncClusterManager deployModuleAs(final String deploymentID, String moduleName, JsonObject config, int instances, final Handler<AsyncResult<String>> doneHandler) {
    platform.deployModule(moduleName, config, instances, true, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(final AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
        } else {
          final String internalID = result.result();

          // Store the deployment ID so that other nodes can reference it for failover.
          vertx.executeBlocking(new Action<Void>() {
            @Override
            public Void perform() {
              deployments.put(deploymentID, nodeID);
              userIDs.put(internalID, deploymentID);
              deploymentIDs.put(deploymentID, internalID);
              return null;
            }
          }, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
              } else {
                new DefaultFutureResult<String>(deploymentID).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager undeployModuleAs(final String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
    // Look up the local deployment ID.
    vertx.executeBlocking(new Action<String>() {
      @Override
      public String perform() {
        return deploymentIDs.get(deploymentID);
      }
    }, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else if (result.result() == null) {
          new DefaultFutureResult<Void>(new IllegalArgumentException("Invalid deployment ID.")).setHandler(doneHandler);
        } else {
          platform.undeploy(result.result(), new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager deployVerticleAs(String deploymentID, String main, JsonObject config,
      int instances, Set<String> includes, Handler<AsyncResult<String>> doneHandler) {
    new DefaultFutureResult<String>(new UnsupportedOperationException("Verticle deployments are not currently supported.")).setHandler(doneHandler);
    return this;
  }

  @Override
  public ZyncClusterManager undeployVerticleAs(String deploymentID,
      Handler<AsyncResult<Void>> doneHandler) {
    new DefaultFutureResult<Void>(new UnsupportedOperationException("Verticle deployments are not currently supported.")).setHandler(doneHandler);
    return this;
  }

  @Override
  public ZyncClusterManager deployWorkerVerticleAs(String deploymentID, String main,
      JsonObject config, int instances, boolean multiThreaded, Set<String> includes,
      Handler<AsyncResult<String>> doneHandler) {
    new DefaultFutureResult<String>(new UnsupportedOperationException("Worker verticle deployments are not currently supported.")).setHandler(doneHandler);
    return this;
  }

  @Override
  public ZyncClusterManager undeployWorkerVerticleAs(String deploymentID,
      Handler<AsyncResult<Void>> doneHandler) {
    new DefaultFutureResult<Void>(new UnsupportedOperationException("Worker verticle deployments are not currently supported.")).setHandler(doneHandler);
    return this;
  }

  @Override
  public ZyncClusterManager set(final String key, final Object value, final Handler<AsyncResult<Void>> doneHandler) {
    data.containsKey(key, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          final boolean existed = result.result();
          data.put(key, value, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                if (existed) {
                  triggerEvent(Event.Type.UPDATE, key, value);
                }
                else {
                  triggerEvent(Event.Type.CREATE, key, value);
                }
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public <T> ZyncClusterManager get(String key, final T defaultValue, final Handler<AsyncResult<T>> resultHandler) {
    data.get(key, new Handler<AsyncResult<Object>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          new DefaultFutureResult<T>(result.cause()).setHandler(resultHandler);
        } else if (result.result() == null) {
          new DefaultFutureResult<T>(defaultValue).setHandler(resultHandler);
        } else {
          new DefaultFutureResult<T>((T) result.result()).setHandler(resultHandler);
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager delete(final String key, final Handler<AsyncResult<Void>> doneHandler) {
    data.get(key, new Handler<AsyncResult<Object>>() {
      @Override
      public void handle(AsyncResult<Object> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          final Object value = result.result();
          data.remove(key, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.failed()) {
                new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
              } else {
                triggerEvent(Event.Type.DELETE, key, value);
                new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager exists(String key, final Handler<AsyncResult<Boolean>> resultHandler) {
    data.containsKey(key, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          new DefaultFutureResult<Boolean>(result.cause()).setHandler(resultHandler);
        } else {
          new DefaultFutureResult<Boolean>(result.result()).setHandler(resultHandler);
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager keys(final Handler<AsyncResult<Set<String>>> resultHandler) {
    data.keySet(new Handler<AsyncResult<Set<String>>>() {
      @Override
      public void handle(AsyncResult<Set<String>> result) {
        if (result.failed()) {
          new DefaultFutureResult<Set<String>>(result.cause()).setHandler(resultHandler);
        } else {
          new DefaultFutureResult<Set<String>>(result.result()).setHandler(resultHandler);
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager watch(String address, String key, Handler<AsyncResult<Void>> doneHandler) {
    return watch(address, key, null, doneHandler);
  }

  @Override
  public ZyncClusterManager watch(final String address, final String key, final Type event, final Handler<AsyncResult<Void>> doneHandler) {
    watchers.get(key, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String swatchers = result.result();
          JsonObject jsonWatchers = swatchers != null ? new JsonObject(swatchers) : new JsonObject();

          if (!jsonWatchers.containsField(address)) {
            jsonWatchers.putArray(address, new JsonArray());
          }

          // Validate that all provided events are valid.
          JsonArray jsonWatcher = jsonWatchers.getArray(address);

          // Only add the event if it doesn't already exist.
          if (!jsonWatcher.contains(event)) {
            jsonWatcher.add(event.toString());
            watchers.put(key, jsonWatchers.encode(), new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                } else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            });
          }
          else {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  @Override
  public ZyncClusterManager unwatch(String address, String key, Handler<AsyncResult<Void>> doneHandler) {
    return unwatch(address, key, null, doneHandler);
  }

  @Override
  public ZyncClusterManager unwatch(final String address, final String key, final Type event, final Handler<AsyncResult<Void>> doneHandler) {
    watchers.get(key, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
          String swatchers = result.result();
          JsonObject jsonWatchers = swatchers != null ? new JsonObject(swatchers) : new JsonObject();

          // If the watcher doesn't exist then simply return ok.
          if (!jsonWatchers.containsField(address)) {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
            return;
          }

          // Validate that all provided events are valid.
          JsonArray jsonWatcher = jsonWatchers.getArray(address);

          // Only remove the event if it actually exists.
          if (jsonWatcher.contains(event.toString())) {
            Iterator<Object> iter = jsonWatcher.iterator();
            while (iter.hasNext()) {
              if (iter.next().equals(event.toString())) {
                iter.remove();
              }
            }

            watchers.put(key, jsonWatchers.encode(), new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                } else {
                  new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
                }
              }
            });
          }
          else {
            new DefaultFutureResult<Void>((Void) null).setHandler(doneHandler);
          }
        }
      }
    });
    return this;
  }

  private void triggerEvent(final Event.Type event, final String key, final Object value) {
    watchers.get(key, new Handler<AsyncResult<String>>() {
      @Override
      public void handle(AsyncResult<String> result) {
        if (result.succeeded() && result.result() != null) {
          JsonObject message = new JsonObject()
              .putString("event", event.toString())
              .putString("key", key)
              .putValue("value", value);

          JsonObject jsonWatchers = new JsonObject(result.result());
          for (String address : jsonWatchers.getFieldNames()) {
            JsonArray jsonWatcher = jsonWatchers.getArray(address);
            if (jsonWatcher.contains(event.toString())) {
              eventBus.send(address, message);
            }
          }
        }
      }
    });
  }

}
