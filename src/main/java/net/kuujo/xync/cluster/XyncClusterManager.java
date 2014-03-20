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
   * Sets a key in the cluster.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster manager.
   */
  XyncClusterManager set(String key, Object value, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Gets a key in the cluster.
   *
   * @param key The key to retrieve.
   * @param def The default value if the key doesn't exist.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster manager.
   */
  <T> XyncClusterManager get(String key, T def, Handler<AsyncResult<T>> resultHandler);

  /**
   * Deletes a key from the cluster.
   *
   * @param key The key to delete.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster manager.
   */
  XyncClusterManager delete(String key, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Checks whether a key exists in the cluster.
   *
   * @param key The key to check.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster manager.
   */
  XyncClusterManager exists(String key, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Watches a key in the cluster for all events.
   *
   * @param key The key to watch.
   * @param handler A handler to be called when an event occurs.
   * @param doneHandler An asynchronous handler to be called once the watcher is registered
   *        in the cluster.
   * @return The cluster manager.
   */
  XyncClusterManager watch(String address, String key, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Watches a key in the cluster for a specific event.
   *
   * @param key The key to watch.
   * @param event The event to watch.
   * @param handler A handler to be called when an event occurs.
   * @param doneHandler An asynchronous handler to be called once the watcher is registered
   *        in the cluster.
   * @return The cluster manager.
   */
  XyncClusterManager watch(String address, String key, Event.Type event, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster for all events.
   *
   * @param key The key to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the watcher is unregistered
   *        from the cluster.
   * @return The cluster manager.
   */
  XyncClusterManager unwatch(String address, String key, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster for a specific event.
   *
   * @param key The key to unwatch.
   * @param event The event to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the watcher is unregistered
   *        from the cluster.
   * @return The cluster manager.
   */
  XyncClusterManager unwatch(String address, String key, Event.Type event, Handler<AsyncResult<Void>> doneHandler);

}
