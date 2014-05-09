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
package net.kuujo.xync.platform.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import net.kuujo.xync.cluster.ClusterManager;
import net.kuujo.xync.platform.PlatformManager;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.VertxException;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Container;
import org.vertx.java.platform.PlatformManagerException;

/**
 * Default Xync platform manager implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultPlatformManager implements PlatformManager {
  private final Logger log = LoggerFactory.getLogger(DefaultPlatformManager.class);
  private static final long QUORUM_CHECK_PERIOD = 1000;

  private final Container container;
  private final ClusterManager manager;
  private final int quorumSize;
  private final String group;
  private final Map<String, String> clusterMap;
  private final String nodeID;
  private final String node;
  private JsonObject haInfo;
  private JsonArray deployments;
  private JsonObject deploymentIDs;
  private JsonObject internalIDs;
  private final Queue<Runnable> toFailoverOnQuorum = new ConcurrentLinkedQueue<>();
  private final Queue<Runnable> toDeployOnQuorum = new ConcurrentLinkedQueue<>();
  private volatile boolean attainedQuorum;

  public DefaultPlatformManager(Vertx vertx, Container container, ClusterManager manager, int quorumSize, String cluster, String group, String node) {
    this.container = container;
    this.manager = manager;
    this.quorumSize = quorumSize;
    this.group = group;
    this.clusterMap = manager.getMap(String.format("cluster.%s", cluster));
    this.nodeID = manager.getNodeId();
    this.node = node;
    manager.joinHandler(new Handler<String>() {
      @Override
      public void handle(String nodeID) {
        DefaultPlatformManager.this.nodeAdded(nodeID);
      }
    });
    manager.leaveHandler(new Handler<String>() {
      @Override
      public void handle(String nodeID) {
        DefaultPlatformManager.this.nodeLeft(nodeID);
      }
    });
    vertx.setPeriodic(QUORUM_CHECK_PERIOD, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        synchronized (DefaultPlatformManager.this) {
          checkQuorum();
        }
        checkHADeployments();
      }
    });
    checkQuorum();
  }

  @Override
  public void start() {
    this.haInfo = new JsonObject();
    haInfo.putString("node", nodeID);
    haInfo.putString("group", group);
    this.deployments = new JsonArray();
    haInfo.putArray("deployments", deployments);
    this.deploymentIDs = new JsonObject();
    haInfo.putObject("external", deploymentIDs);
    this.internalIDs = new JsonObject();
    haInfo.putObject("internal", internalIDs);
    clusterMap.put(node, haInfo.encode());
  }

  @Override
  public void stop() {
    clusterMap.remove(node);
  }

  @Override
  public void isDeployed(final String deploymentID, final Handler<AsyncResult<Boolean>> resultHandler) {
    Set<String> nodes = manager.getNodes();
    for (Map.Entry<String, String> entry : clusterMap.entrySet()) {
      JsonObject haInfo = new JsonObject(entry.getValue());
      String nodeID = haInfo.getString("node");
      JsonArray deployments = haInfo.getArray("deployments");
      for (Object deployment : deployments) {
        JsonObject deploymentInfo = (JsonObject) deployment;
        if (deploymentInfo.getString("id").equals(deploymentID) && (nodes.contains(nodeID) || deploymentInfo.getBoolean("ha", false))) {
          new DefaultFutureResult<Boolean>(true).setHandler(resultHandler);
          return;
        }
      }
    }
    new DefaultFutureResult<Boolean>(false).setHandler(resultHandler);
  }

  @Override
  public void getAssignment(final String deploymentID, final Handler<AsyncResult<String>> resultHandler) {
    for (Map.Entry<String, String> entry : clusterMap.entrySet()) {
      JsonObject haInfo = new JsonObject(entry.getValue());
      JsonObject internalIDs = haInfo.getObject("internal");
      if (internalIDs != null && internalIDs.containsField(deploymentID)) {
        new DefaultFutureResult<String>(entry.getKey()).setHandler(resultHandler);
        return;
      }
    }
    new DefaultFutureResult<String>(new VertxException("Invalid deployment ID.")).setHandler(resultHandler);
  }

  @Override
  public void getDeploymentInfo(final String deploymentID, final Handler<AsyncResult<JsonObject>> resultHandler) {
    for (Map.Entry<String, String> entry : clusterMap.entrySet()) {
      JsonObject haInfo = new JsonObject(entry.getValue());
      JsonArray deployments = haInfo.getArray("deployments");
      if (deployments != null) {
        for (Object dep : deployments) {
          JsonObject deployment = (JsonObject) dep;
          if (deployment.getString("id").equals(deploymentID)) {
            new DefaultFutureResult<JsonObject>(deployment.putString("node", entry.getKey())).setHandler(resultHandler);
            return;
          }
        }
      }
    }
    new DefaultFutureResult<JsonObject>(new VertxException("Invalid deployment ID.")).setHandler(resultHandler);
  }

  @Override
  public void deployModuleAs(final String deploymentID, final String moduleName, final JsonObject config, final int instances,
                                        final boolean ha, final Handler<AsyncResult<String>> doneHandler) {
    if (attainedQuorum || !ha) {
      // Don't deploy the module if a deployment with the same ID already exists.
      isDeployed(deploymentID, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
          } else if (result.result()) {
            new DefaultFutureResult<String>(new PlatformManagerException("Deployment already exists.")).setHandler(doneHandler);
          } else {
            container.deployModule(moduleName, config, instances, new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                  // Tell the other nodes of the cluster about the module for HA purposes
                  addModuleToHA(deploymentID, result.result(), moduleName, config, instances, ha);
                }
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
    } else {
      log.info("Quorum not attained. Deployment of module will be delayed until there's a quorum.");
      addModuleToHADeployList(deploymentID, moduleName, config, instances, doneHandler);
    }
  }

  @Override
  public void deployVerticleAs(final String deploymentID, final String main, final JsonObject config, final int instances,
      final boolean ha, final Handler<AsyncResult<String>> doneHandler) {
    if (attainedQuorum || !ha) {
      // Don't deploy the verticle if a deployment with the same ID already exists.
      isDeployed(deploymentID, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
          } else if (result.result()) {
            new DefaultFutureResult<String>(new PlatformManagerException("Deployment already exists.")).setHandler(doneHandler);
          } else {
            container.deployVerticle(main, config, instances, new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                  // Tell the other nodes of the cluster about the verticle for HA purposes
                  addVerticleToHA(deploymentID, result.result(), main, config, instances, ha);
                }
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
    } else {
      log.info("Quorum not attained. Deployment of verticle will be delayed until there's a quorum.");
      addVerticleToHADeployList(deploymentID, main, config, instances, doneHandler);
    }
  }

  @Override
  public void deployWorkerVerticleAs(final String deploymentID, final String main, final JsonObject config, final int instances,
      final boolean multiThreaded, final boolean ha, final Handler<AsyncResult<String>> doneHandler) {
    if (attainedQuorum || !ha) {
      // Don't deploy the module if a deployment with the same ID already exists.
      isDeployed(deploymentID, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<String>(result.cause()).setHandler(doneHandler);
          } else if (result.result()) {
            new DefaultFutureResult<String>(new PlatformManagerException("Deployment already exists.")).setHandler(doneHandler);
          } else {
            container.deployWorkerVerticle(main, config, instances, multiThreaded, new Handler<AsyncResult<String>>() {
              @Override
              public void handle(AsyncResult<String> result) {
                if (result.succeeded()) {
                  // Tell the other nodes of the cluster about the verticle for HA purposes
                  addWorkerVerticleToHA(deploymentID, result.result(), main, config, instances, multiThreaded, ha);
                }
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
    } else {
      log.info("Quorum not attained. Deployment of verticle will be delayed until there's a quorum.");
      addWorkerVerticleToHADeployList(deploymentID, main, config, instances, multiThreaded, doneHandler);
    }
  }

  @Override
  public void undeployModuleAs(final String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
    synchronized (haInfo) {
      // Undeploy the deployment.
      String internalID = internalIDs.getString(deploymentID);
      internalIDs.removeField(deploymentID);
      if (internalID != null) {
        container.undeployModule(internalID, doneHandler);
        deploymentIDs.removeField(internalID);
      } else {
        new DefaultFutureResult<Void>(new PlatformManagerException("Invalid deployment.")).setHandler(doneHandler);
      }

      // Remove the deployment from HA.
      Iterator<Object> iter = deployments.iterator();
      while (iter.hasNext()) {
        JsonObject deploymentInfo = (JsonObject) iter.next();
        if (deploymentInfo.getString("id").equals(deploymentID)) {
          iter.remove();
        }
      }
    }
    clusterMap.put(node, haInfo.encode());
  }

  @Override
  public void undeployVerticleAs(final String deploymentID, final Handler<AsyncResult<Void>> doneHandler) {
    synchronized (haInfo) {
      // Undeploy the deployment.
      String internalID = internalIDs.getString(deploymentID);
      internalIDs.removeField(deploymentID);
      if (internalID != null) {
        container.undeployVerticle(internalID, doneHandler);
        deploymentIDs.removeField(internalID);
      } else {
        new DefaultFutureResult<Void>(new PlatformManagerException("Invalid deployment.")).setHandler(doneHandler);
      }

      // Remove the deployment from HA.
      Iterator<Object> iter = deployments.iterator();
      while (iter.hasNext()) {
        JsonObject deploymentInfo = (JsonObject) iter.next();
        if (deploymentInfo.getString("id").equals(deploymentID)) {
          iter.remove();
        }
      }
    }
    clusterMap.put(node, haInfo.encode());
  }

  // A node has joined the cluster
  // synchronize this in case the cluster manager is naughty and calls it concurrently
  private synchronized void nodeAdded(final String nodeID) {
    
  }

  // A node has left the cluster
  // synchronize this in case the cluster manager is naughty and calls it concurrently
  private synchronized void nodeLeft(String leftNodeID) {
    Set<String> nodes = manager.getNodes();
    for (final Map.Entry<String, String> entry : clusterMap.entrySet()) {
      final JsonObject haInfo = new JsonObject(entry.getValue());
      String nodeID = haInfo.getString("node");
      if (nodeID != null && (nodeID.equals(leftNodeID) || !nodes.contains(nodeID))) {
        toFailoverOnQuorum.add(new Runnable() {
          @Override
          public void run() {
            checkFailover(entry.getKey(), haInfo);
          }
        });
      }
    }
  }

  // Check if there is a quorum for our group
  private void checkQuorum() {
    int count = 0;
    if (quorumSize <= 1) {
      count = 1;
    } else {
      Set<String> nodes = manager.getNodes();
  
      for (Map.Entry<String, String> entry : clusterMap.entrySet()) {
        JsonObject haInfo = new JsonObject(entry.getValue());
        String nodeID = haInfo.getString("node");
        if (nodeID != null && nodes.contains(nodeID)) {
          String group = haInfo.getString("group");
          if (group != null && group.equals(this.group)) {
            count++;
          }
        }
      }
    }

    boolean attained = count >= quorumSize;
    if (!attainedQuorum && attained) {
      // A quorum has been attained so we can deploy any currently undeployed HA deployments
      log.info("A quorum has been obtained. Any deployments waiting on a quorum will now be deployed");
      this.attainedQuorum = true;
    } else if (attainedQuorum && !attained) {
      // We had a quorum but we lost it - we must undeploy any HA deployments
      log.info("There is no longer a quorum. Any HA deployments will be undeployed until a quorum is re-attained");
      this.attainedQuorum = false;
    }
  }

  // Add some information on a deployment in the cluster so other nodes know about it
  private void addModuleToHA(String deploymentID, String internalID, String moduleName, JsonObject conf, int instances, boolean ha) {
    JsonObject info = new JsonObject()
        .putString("id", deploymentID)
        .putString("type", "module")
        .putString("group", group)
        .putString("module", moduleName)
        .putObject("config", conf)
        .putNumber("instances", instances)
        .putBoolean("ha", ha);
    synchronized (haInfo) {
      deployments.addObject(info);
      deploymentIDs.putString(internalID, deploymentID);
      internalIDs.putString(deploymentID, internalID);
    }
    clusterMap.put(node, haInfo.encode());
  }

  // Add some information on a deployment in the cluster so other nodes know about it
  private void addVerticleToHA(String deploymentID, String internalID, String main, JsonObject conf, int instances, boolean ha) {
    JsonObject info = new JsonObject()
        .putString("id", deploymentID)
        .putString("type", "verticle")
        .putString("group", group)
        .putString("main", main)
        .putObject("config", conf)
        .putNumber("instances", instances)
        .putBoolean("ha", ha);
    synchronized (haInfo) {
      deployments.addObject(info);
      deploymentIDs.putString(internalID, deploymentID);
      internalIDs.putString(deploymentID, internalID);
    }
    clusterMap.put(node, haInfo.encode());
  }

  // Add some information on a deployment in the cluster so other nodes know about it
  private void addWorkerVerticleToHA(String deploymentID, String internalID, String main, JsonObject conf, int instances, boolean multiThreaded, boolean ha) {
    JsonObject info = new JsonObject()
        .putString("id", deploymentID)
        .putString("type", "verticle")
        .putString("group", group)
        .putString("main", main)
        .putObject("config", conf)
        .putNumber("instances", instances)
        .putBoolean("worker", true)
        .putBoolean("multi-threaded", multiThreaded)
        .putBoolean("ha", ha);
    synchronized (haInfo) {
      deployments.addObject(info);
      deploymentIDs.putString(internalID, deploymentID);
      internalIDs.putString(deploymentID, internalID);
    }
    clusterMap.put(node, haInfo.encode());
  }

  // Add the deployment to an internal list of deployments - these will be executed when a quorum is attained
  private void addModuleToHADeployList(final String deploymentID, final String moduleName, final JsonObject config, final int instances,
                                 final Handler<AsyncResult<String>> doneHandler) {
    toDeployOnQuorum.add(new Runnable() {
      public void run() {
        deployModuleAs(deploymentID, moduleName, config, instances, true, doneHandler);
      }
    });
   }

  // Add the deployment to an internal list of deployments - these will be executed when a quorum is attained
  private void addVerticleToHADeployList(final String deploymentID, final String main, final JsonObject config, final int instances, final Handler<AsyncResult<String>> doneHandler) {
    toDeployOnQuorum.add(new Runnable() {
      public void run() {
        deployVerticleAs(deploymentID, main, config, instances, true, doneHandler);
      }
    });
  }

  // Add the deployment to an internal list of deployments - these will be executed when a quorum is attained
  private void addWorkerVerticleToHADeployList(final String deploymentID, final String main, final JsonObject config, final int instances,
                                 final boolean multiThreaded, final Handler<AsyncResult<String>> doneHandler) {
    toDeployOnQuorum.add(new Runnable() {
      public void run() {
        deployWorkerVerticleAs(deploymentID, main, config, instances, multiThreaded, true, doneHandler);
      }
    });
  }

  private void checkHADeployments() {
    try {
      if (attainedQuorum) {
        deployHADeployments();
      } else {
        undeployHADeployments();
      }
    } catch (Throwable t) {
      log.error("Failed when checking HA deployments", t);
    }
  }

  // Undeploy any HA deployments now there is no quorum
  private void undeployHADeployments() {
    for (Object deployment : deployments) {
      final JsonObject deploymentInfo = (JsonObject) deployment;
      if (deploymentInfo.getBoolean("ha", false)) {
        String type = deploymentInfo.getString("type");
        if (type.equals("module")) {
          final String deploymentID = internalIDs.getString(deploymentInfo.getString("id"));
          if (deploymentID != null) {
            container.undeployModule(deploymentID, new AsyncResultHandler<Void>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.succeeded()) {
                  log.info("Successfully undeployed HA deployment " + deploymentInfo.getString("id") + " as there is no quorum");
                  addModuleToHADeployList(deploymentID, deploymentInfo.getString("module"), deploymentInfo.getObject("config"),
                      deploymentInfo.getInteger("instances"), new AsyncResultHandler<String>() {
                    @Override
                    public void handle(AsyncResult<String> result) {
                      if (result.succeeded()) {
                        log.info("Successfully redeployed module " + deploymentInfo.getString("module") + " after quorum was re-attained");
                      } else {
                        log.error("Failed to redeploy module " + deploymentInfo.getString("module") + " after quorum was re-attained", result.cause());
                      }
                    }
                  });
                } else {
                  log.error("Failed to undeploy deployment on lost quorum", result.cause());
                }
              }
            });
          }
        }
        else if (type.equals("verticle")) {
          boolean isWorker = deploymentInfo.getBoolean("worker", false);
          if (isWorker) {
            final String deploymentID = internalIDs.getString(deploymentInfo.getString("id"));
            if (deploymentID != null) {
              container.undeployVerticle(deploymentID, new AsyncResultHandler<Void>() {
                @Override
                public void handle(AsyncResult<Void> result) {
                  if (result.succeeded()) {
                    log.info("Successfully undeployed HA deployment " + deploymentInfo.getString("id") + " as there is no quorum");
                    addWorkerVerticleToHADeployList(deploymentID, deploymentInfo.getString("main"), deploymentInfo.getObject("config"),
                        deploymentInfo.getInteger("instances"), deploymentInfo.getBoolean("multi-threaded", false), new AsyncResultHandler<String>() {
                      @Override
                      public void handle(AsyncResult<String> result) {
                        if (result.succeeded()) {
                          log.info("Successfully redeployed worker verticle " + deploymentInfo.getString("main") + " after quorum was re-attained");
                        } else {
                          log.error("Failed to redeploy worker verticle " + deploymentInfo.getString("main") + " after quorum was re-attained", result.cause());
                        }
                      }
                    });
                  } else {
                    log.error("Failed to undeploy deployment on lost quorum", result.cause());
                  }
                }
              });
            }
          }
          else {
            final String deploymentID = internalIDs.getString(deploymentInfo.getString("id"));
            if (deploymentID != null) {
              container.undeployVerticle(deploymentID, new AsyncResultHandler<Void>() {
                @Override
                public void handle(AsyncResult<Void> result) {
                  if (result.succeeded()) {
                    log.info("Successfully undeployed HA deployment " + deploymentInfo.getString("id") + " as there is no quorum");
                    addVerticleToHADeployList(deploymentID, deploymentInfo.getString("main"), deploymentInfo.getObject("config"),
                        deploymentInfo.getInteger("instances"), new AsyncResultHandler<String>() {
                      @Override
                      public void handle(AsyncResult<String> result) {
                        if (result.succeeded()) {
                          log.info("Successfully redeployed verticle " + deploymentInfo.getString("main") + " after quorum was re-attained");
                        } else {
                          log.error("Failed to redeploy verticle " + deploymentInfo.getString("main") + " after quorum was re-attained", result.cause());
                        }
                      }
                    });
                  } else {
                    log.error("Failed to undeploy deployment on lost quorum", result.cause());
                  }
                }
              });
            }
          }
        }
      }
    }
  }

  // Deploy any deployments that are waiting for a quorum
  private void deployHADeployments() {
    Runnable task;
    while ((task = toFailoverOnQuorum.poll()) != null) {
      try {
        task.run();
      } catch (Throwable t) {
        log.error("Failed to run failover task", t);
      }
    }

    int size = toDeployOnQuorum.size();
    if (size != 0) {
      log.info("There are " + size + " HA deployments waiting on a quorum. These will now be deployed");
      while ((task = toDeployOnQuorum.poll()) != null) {
        try {
          task.run();
        } catch (Throwable t) {
          log.error("Failed to run redeployment task", t);
        }
      }
    }
  }

  // Handle failover
  private void checkFailover(String failedNodeID, JsonObject theHAInfo) {
    try {
      JsonArray deployments = theHAInfo.getArray("deployments");
      String group = theHAInfo.getString("group");
      String chosen = chooseHashedNode(group, failedNodeID.hashCode());
      if (chosen != null && chosen.equals(this.node)) {
        log.info("Node " + failedNodeID + " has failed. This node will deploy " + deployments.size() + " deployments from that node.");
        if (deployments != null) {
          for (Object obj: deployments) {
            JsonObject deployment = (JsonObject)obj;
            if (deployment.getBoolean("ha", false)) {
              processFailover(deployment);
            }
          }
        }
        // Failover is complete! We can now remove the failed node from the cluster map
        clusterMap.remove(failedNodeID);
      }
    } catch (Throwable t) {
      log.error("Failed to handle failover", t);
    }
  }

  // Process the failover of a deployment
  private void processFailover(final JsonObject deploymentInfo) {
    final String type = deploymentInfo.getString("type");
    if (type.equals("module")) {
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Throwable> err = new AtomicReference<>();
      container.deployModule(deploymentInfo.getString("module"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), new Handler<AsyncResult<String>>() {
        @Override
        public void handle(AsyncResult<String> result) {
          if (result.succeeded()) {
            // Tell the other nodes of the cluster about the module for HA purposes
            addModuleToHA(deploymentInfo.getString("id"), result.result(), deploymentInfo.getString("module"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), true);
            log.info("Successfully redeployed module " + deploymentInfo.getString("module") + " after failover");
          } else {
            log.error("Failed to redeploy module after failover", result.cause());
            err.set(result.cause());
          }
          latch.countDown();
          Throwable t = err.get();
          if (t != null) {
            throw new VertxException(t);
          }
        }
      });
      try {
        if (!latch.await(120, TimeUnit.SECONDS)) {
          throw new VertxException("Timed out waiting for redeploy on failover");
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    else if (type.equals("verticle")) {
      final boolean isWorker = deploymentInfo.getBoolean("worker", false);
      if (isWorker) {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> err = new AtomicReference<>();
        container.deployWorkerVerticle(deploymentInfo.getString("main"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), deploymentInfo.getBoolean("multi-threaded", false), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.succeeded()) {
              // Tell the other nodes of the cluster about the verticle for HA purposes
              addWorkerVerticleToHA(deploymentInfo.getString("id"), result.result(), deploymentInfo.getString("main"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), deploymentInfo.getBoolean("multi-threaded", false), true);
              log.info("Successfully redeployed verticle " + deploymentInfo.getString("main") + " after failover");
            } else {
              log.error("Failed to redeploy verticle after failover", result.cause());
              err.set(result.cause());
            }
            latch.countDown();
            Throwable t = err.get();
            if (t != null) {
              throw new VertxException(t);
            }
          }
        });
        try {
          if (!latch.await(120, TimeUnit.SECONDS)) {
            throw new VertxException("Timed out waiting for redeploy on failover");
          }
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      } else {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> err = new AtomicReference<>();
        container.deployVerticle(deploymentInfo.getString("main"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), new Handler<AsyncResult<String>>() {
          @Override
          public void handle(AsyncResult<String> result) {
            if (result.succeeded()) {
              // Tell the other nodes of the cluster about the verticle for HA purposes
              addVerticleToHA(deploymentInfo.getString("id"), result.result(), deploymentInfo.getString("main"), deploymentInfo.getObject("config"), deploymentInfo.getInteger("instances", 1), true);
              log.info("Successfully redeployed verticle " + deploymentInfo.getString("main") + " after failover");
            } else {
              log.error("Failed to redeploy verticle after failover", result.cause());
              err.set(result.cause());
            }
            latch.countDown();
            Throwable t = err.get();
            if (t != null) {
              throw new VertxException(t);
            }
          }
        });
        try {
          if (!latch.await(120, TimeUnit.SECONDS)) {
            throw new VertxException("Timed out waiting for redeploy on failover");
          }
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
    }
  }

  // Compute the failover node
  private String chooseHashedNode(String group, int hashCode) {
    ArrayList<String> matchingMembers = new ArrayList<>();
    for (Map.Entry<String, String> entry : clusterMap.entrySet()) {
      JsonObject haInfo = new JsonObject(entry.getValue());
      if (haInfo.containsField("group") && haInfo.getString("group").equals(group)) {
        matchingMembers.add(entry.getKey());
      }
    }
    if (!matchingMembers.isEmpty()) {
      // Hashcodes can be -ve so make it positive
      long absHash = (long)hashCode + Integer.MAX_VALUE;
      long lpos = absHash % matchingMembers.size();
      return matchingMembers.get((int)lpos);
    } else {
      return null;
    }
  }

}
