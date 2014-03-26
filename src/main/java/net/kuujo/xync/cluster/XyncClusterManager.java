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
package net.kuujo.xync.cluster;

import java.net.URL;
import java.util.Set;

import net.kuujo.xync.cluster.data.AsyncIdGenerator;
import net.kuujo.xync.cluster.data.AsyncList;
import net.kuujo.xync.cluster.data.AsyncLock;
import net.kuujo.xync.cluster.data.AsyncQueue;
import net.kuujo.xync.cluster.data.AsyncSet;
import net.kuujo.xync.cluster.data.WatchableAsyncMap;
import net.kuujo.xync.platform.XyncPlatformManager;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.spi.cluster.ClusterManager;

/**
 * Xync cluster manager.
 *
 * @author Jordan Halterman
 */
public interface XyncClusterManager {

  /**
   * Returns the cluster manager address.
   *
   * @return The cluster manager address.
   */
  String address();

  /**
   * Returns the cluster manager group.
   *
   * @return The cluster manager group.
   */
  String group();

  /**
   * Returns the internal Vert.x instance.
   *
   * @return The vertx instance.
   */
  VertxInternal vertx();

  /**
   * Returns the current platform manager.
   *
   * @return The internal platform manager.
   */
  XyncPlatformManager platform();

  /**
   * Returns the current Vert.x cluster manager.
   *
   * @return The Vert.x cluster manager.
   */
  ClusterManager cluster();

  /**
   * Starts the cluster manager.
   *
   * @param doneHandler An asynchronous handler to be called once started.
   * @return The cluster manager.
   */
  XyncClusterManager start(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Stops the cluster manager.
   *
   * @param doneHandler An asynchronous handler to be called once stopped.
   */
  void stop(Handler<AsyncResult<Void>> doneHandler);

  /**
   * Checks if a deployment is deployed.
   *
   * @param deploymentID The deployment ID to check.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster manager.
   */
  XyncClusterManager isDeployed(String deploymentID, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Returns deployment info.
   *
   * @param deploymentID The deployment ID.
   * @param resultHandler An asynchronous handler to be called with the deployment info.
   * @return The cluster manager.
   */
  XyncClusterManager getDeploymentInfo(String deploymentID, Handler<AsyncResult<DeploymentInfo>> resultHandler);

  /**
   * Deploys a module to the cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the module is deployed.
   * @return The cluster manager.
   */
  XyncClusterManager deployModuleAs(String deploymentID, String moduleName, JsonObject config, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a module in the cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param doneHandler An asynchronous handler to be called once the module is undeployed.
   * @return The cluster manager.
   */
  XyncClusterManager undeployModuleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the verticle is deployed.
   * @return The cluster manager.
   */
  XyncClusterManager deployVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, String includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the verticle is deployed.
   * @return The cluster manager.
   */
  XyncClusterManager deployVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, Set<String> includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a verticle in the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param doneHandler An asynchronous handler to be called once the verticle is undeployed.
   * @return The cluster manager.
   */
  XyncClusterManager undeployVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the verticle is deployed.
   * @return The cluster manager.
   */
  XyncClusterManager deployWorkerVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, boolean multiThreaded, String includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle to the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Whether the worker is multi-threaded.
   * @param doneHandler An asynchronous handler to be called once the verticle is deployed.
   * @return The cluster manager.
   */
  XyncClusterManager deployWorkerVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, boolean multiThreaded, Set<String> includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a worker verticle in the cluster.
   *
   * @param deploymentID The verticle deployment ID.
   * @param doneHandler An asynchronous handler to be called once the verticle is undeployed.
   * @return The cluster manager.
   */
  XyncClusterManager undeployWorkerVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Returns an asynchronous cluster-wide replicated map.
   *
   * @param name The map name.
   * @return An asynchronous cluster-wide replicated map.
   */
  <K, V> WatchableAsyncMap<K, V> getMap(String name);

  /**
   * Returns an asynchronous cluster-wide replicated list.
   *
   * @param name The list name.
   * @return An asynchronous cluster-wide replicated list.
   */
  <T> AsyncList<T> getList(String name);

  /**
   * Returns an asynchronous cluster-wide replicated set.
   *
   * @param name The set name.
   * @return An asynchronous cluster-wide replicated set.
   */
  <T> AsyncSet<T> getSet(String name);

  /**
   * Returns an asynchronous cluster-wide replicated queue.
   *
   * @param name The queue name.
   * @return An asynchronous cluster-wide replicated queue.
   */
  <T> AsyncQueue<T> getQueue(String name);

  /**
   * Returns an asynchronous cluster-wide unique ID generator.
   *
   * @param name The ID generator name.
   * @return An asynchronous cluster-wide ID generator.
   */
  AsyncIdGenerator getIdGenerator(String name);

  /**
   * Returns an asynchronous cluster-wide lock.
   *
   * @param name The lock name.
   * @return An asynchronous cluster-wide lock.
   */
  AsyncLock getLock(String name);

}
