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

import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.kuujo.xync.cluster.DeploymentInfo;
import net.kuujo.xync.cluster.Event;
import net.kuujo.xync.cluster.Event.Type;
import net.kuujo.xync.cluster.ModuleDeploymentInfo;
import net.kuujo.xync.cluster.VerticleDeploymentInfo;
import net.kuujo.xync.cluster.WorkerVerticleDeploymentInfo;
import net.kuujo.xync.cluster.XyncClusterManager;
import net.kuujo.xync.platform.XyncPlatformManager;

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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IdGenerator;

/**
 * Default cluster manager implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultXyncClusterManager implements XyncClusterManager {
  private static final String CLUSTER_MAP_NAME = "__xync.cluster";
  private static final String DEFAULT_GROUP = "__DEFAULT__";
  private static final String CLUSTER_KEY_PREFIX = "__xync.";

  private final String nodeID;
  private final String group;
  private final VertxInternal vertx;
  private final XyncPlatformManager platform;
  private final EventBus eventBus;
  private final ClusterManager clusterManager;
  private HazelcastInstance hazelcast;
  private final Map<String, String> cluster;

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

  public DefaultXyncClusterManager(String group, VertxInternal vertx, XyncPlatformManager platform, ClusterManager clusterManager) {
    this.nodeID = clusterManager.getNodeID();
    this.group = group != null ? group : DEFAULT_GROUP;
    this.vertx = vertx;
    this.platform = platform;
    this.eventBus = vertx.eventBus();
    this.clusterManager = clusterManager;
    this.cluster = clusterManager.getSyncMap(CLUSTER_MAP_NAME);
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
  public XyncPlatformManager platform() {
    return platform;
  }

  @Override
  public ClusterManager cluster() {
    return clusterManager;
  }

  @Override
  @SuppressWarnings("deprecation")
  public XyncClusterManager start(final Handler<AsyncResult<Void>> doneHandler) {
    hazelcast = Hazelcast.getDefaultInstance();
    eventBus.registerHandler(nodeID, internalHandler, new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        if (result.failed()) {
          new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
        } else {
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
        } else {
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
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment type."));
      return;
    }

    // If the deployment info indicates a group other than the current cluster
    // group then return an error.
    String group = message.body().getString("group");
    if (group == null) {
      group = DEFAULT_GROUP;
    }

    if (!group.equals(this.group)) {
      message.reply(new JsonObject().putString("status", "error").putString("message", "Invalid deployment group."));
      return;
    }

    DeploymentInfo.Type type = DeploymentInfo.Type.parse(stype);
    if (type.equals(DeploymentInfo.Type.MODULE)) {
      final ModuleDeploymentInfo deploymentInfo = new DefaultModuleDeploymentInfo(message.body());

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
    } else if (type.equals(DeploymentInfo.Type.VERTICLE)) {
      boolean isWorker = message.body().getBoolean("worker", false);
      if (isWorker) {
        final WorkerVerticleDeploymentInfo deploymentInfo = new DefaultWorkerVerticleDeploymentInfo(message.body());

        deployWorkerVerticleAs(deploymentInfo.id(), deploymentInfo.main(), deploymentInfo.config(), deploymentInfo.classpath(), deploymentInfo.instances(), deploymentInfo.isMultiThreaded(), deploymentInfo.includes(), new Handler<AsyncResult<String>>() {
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

        deployVerticleAs(deploymentInfo.id(), deploymentInfo.main(), deploymentInfo.config(), deploymentInfo.classpath(), deploymentInfo.instances(), deploymentInfo.includes(), new Handler<AsyncResult<String>>() {
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
        for (Map.Entry<String, String> entry : DefaultXyncClusterManager.this.cluster.entrySet()) {
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
  public XyncClusterManager deployModuleAs(final String deploymentID, String moduleName, JsonObject config, int instances, final Handler<AsyncResult<String>> doneHandler) {
    platform.deployModuleAs(deploymentID, moduleName, config, instances, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager undeployModuleAs(final String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
    platform.undeployModuleAs(deploymentID, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager deployVerticleAs(String deploymentID, String main, JsonObject config,
      URL[] classpath, int instances, Set<String> includes, Handler<AsyncResult<String>> doneHandler) {
    return deployVerticleAs(deploymentID, main, config, classpath, instances, setToString(includes), doneHandler);
  }

  @Override
  public XyncClusterManager deployVerticleAs(String deploymentID, String main, JsonObject config,
      URL[] classpath, int instances, String includes, Handler<AsyncResult<String>> doneHandler) {
    platform.deployVerticleAs(deploymentID, main, config, classpath, instances, includes, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager undeployVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    platform.undeployVerticleAs(deploymentID, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager deployWorkerVerticleAs(String deploymentID, String main,
      JsonObject config, URL[] classpath, int instances, boolean multiThreaded, Set<String> includes,
      Handler<AsyncResult<String>> doneHandler) {
    return deployWorkerVerticleAs(deploymentID, main, config, classpath, instances, multiThreaded, setToString(includes), doneHandler);
  }

  @Override
  public XyncClusterManager deployWorkerVerticleAs(String deploymentID, String main,
      JsonObject config, URL[] classpath, int instances, boolean multiThreaded, String includes,
      Handler<AsyncResult<String>> doneHandler) {
    platform.deployWorkerVerticleAs(deploymentID, main, config, classpath, instances, multiThreaded, includes, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager undeployWorkerVerticleAs(String deploymentID,
      Handler<AsyncResult<Void>> doneHandler) {
    new DefaultFutureResult<Void>(new UnsupportedOperationException("Worker verticle deployments are not currently supported.")).setHandler(doneHandler);
    return this;
  }

  private static String setToString(Set<String> set) {
    StringBuilder sset = new StringBuilder();
    for (String item : set) {
      if (sset.length() == 0) {
        sset.append(item);
      } else {
        sset.append(",");
        sset.append(item);
      }
    }
    return sset.toString();
  }

  @Override
  public XyncClusterManager generateId(String name, final Handler<AsyncResult<Long>> doneHandler) {
    final IdGenerator generator = hazelcast.getIdGenerator(name);
    vertx.executeBlocking(new Action<Long>() {
      @Override
      public Long perform() {
        return generator.newId();
      }
    }, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager set(final String name, final String key, final Object value, final Handler<AsyncResult<Void>> doneHandler) {
    final XyncAsyncMap<String, Object> data = new XyncAsyncMap<String, Object>(vertx, clusterManager.<String, Object>getSyncMap(createHazelcastName(name)));
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
                triggerEvent(name, key, Event.Type.CHANGE, value);
                if (existed) {
                  triggerEvent(name, key, Event.Type.UPDATE, value);
                }
                else {
                  triggerEvent(name, key, Event.Type.CREATE, value);
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
  public <T> XyncClusterManager get(String name, String key, final T defaultValue, final Handler<AsyncResult<T>> resultHandler) {
    final XyncAsyncMap<String, Object> data = new XyncAsyncMap<String, Object>(vertx, clusterManager.<String, Object>getSyncMap(createHazelcastName(name)));
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
  public XyncClusterManager delete(final String name, final String key, final Handler<AsyncResult<Void>> doneHandler) {
    final XyncAsyncMap<String, Object> data = new XyncAsyncMap<String, Object>(vertx, clusterManager.<String, Object>getSyncMap(createHazelcastName(name)));
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
                triggerEvent(name, key, Event.Type.CHANGE, value);
                triggerEvent(name, key, Event.Type.DELETE, value);
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
  public XyncClusterManager exists(String name, String key, final Handler<AsyncResult<Boolean>> resultHandler) {
    final XyncAsyncMap<String, Object> data = new XyncAsyncMap<String, Object>(vertx, clusterManager.<String, Object>getSyncMap(createHazelcastName(name)));
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
  public XyncClusterManager watch(String name, String key, String address, Handler<AsyncResult<Void>> doneHandler) {
    return watch(name, key, address, null, doneHandler);
  }

  @Override
  public XyncClusterManager watch(final String name, final String key, final String address, final Type event, final Handler<AsyncResult<Void>> doneHandler) {
    final XyncAsyncMap<String, String> watchers = new XyncAsyncMap<String, String>(vertx, clusterManager.<String, String>getSyncMap(String.format("__xync.watchers.%s", name)));
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
  public XyncClusterManager unwatch(String name, String key, String address, Handler<AsyncResult<Void>> doneHandler) {
    return unwatch(name, key, address, null, doneHandler);
  }

  @Override
  public XyncClusterManager unwatch(final String name, final String key, final String address, final Type event, final Handler<AsyncResult<Void>> doneHandler) {
    final XyncAsyncMap<String, String> watchers = new XyncAsyncMap<String, String>(vertx, clusterManager.<String, String>getSyncMap(String.format("__xync.watchers.%s", name)));
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

  private void triggerEvent(final String name, final String key, final Event.Type event, final Object value) {
    final XyncAsyncMap<String, String> watchers = new XyncAsyncMap<String, String>(vertx, clusterManager.<String, String>getSyncMap(String.format("__xync.watchers.%s", name)));
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

  @Override
  public XyncClusterManager add(final String name, final Object value, final Handler<AsyncResult<Void>> doneHandler) {
    final IList<Object> list = hazelcast.getList(createHazelcastName(name));
    vertx.executeBlocking(new Action<Void>() {
      @Override
      public Void perform() {
        list.add(value);
        return null;
      } 
    }, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager remove(String name, final int index, final Handler<AsyncResult<Object>> doneHandler) {
    final IList<Object> list = hazelcast.getList(createHazelcastName(name));
    vertx.executeBlocking(new Action<Object>() {
      @Override
      public Object perform() {
        return list.remove(index);
      }
    }, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager remove(String name, final Object value, final Handler<AsyncResult<Boolean>> doneHandler) {
    final IList<Object> list = hazelcast.getList(createHazelcastName(name));
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return list.remove(value);
      }
    }, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager contains(String name, final Object value, final Handler<AsyncResult<Boolean>> doneHandler) {
    final IList<Object> list = hazelcast.getList(createHazelcastName(name));
    vertx.executeBlocking(new Action<Boolean>() {
      @Override
      public Boolean perform() {
        return list.contains(value);
      }
    }, doneHandler);
    return this;
  }

  @Override
  public XyncClusterManager count(String name, final Handler<AsyncResult<Integer>> doneHandler) {
    final IList<Object> list = hazelcast.getList(createHazelcastName(name));
    vertx.executeBlocking(new Action<Integer>() {
      @Override
      public Integer perform() {
        return list.size();
      }
    }, doneHandler);
    return this;
  }

  private static String createHazelcastName(String name) {
    return String.format("%s%s", CLUSTER_KEY_PREFIX, name);
  }

}
