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

import java.util.Set;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * Xing cluster client.
 *
 * @author Jordan Halterman
 */
public interface XyncClusterClient {

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModule(String deploymentID, String moduleName, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModule(String deploymentID, String moduleName, JsonObject config, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModule(String deploymentID, String moduleName, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModule(String deploymentID, String moduleName, JsonObject config, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param group The group to which to deploy the module.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param group The group to which to deploy the module.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName, JsonObject config, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param group The group to which to deploy the module.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a module to the xync cluster.
   *
   * @param deploymentID The module deployment ID.
   * @param group The group to which to deploy the module.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once the deployment is complete.
   * @return The xync client.
   */
  XyncClusterClient deployModuleTo(String deploymentID, String group, String moduleName, JsonObject config, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a module.
   *
   * @param deploymentID The module deployment ID.
   * @param doneHandler An asynchronous handler to be called once the module is undeployed.
   * @return The cluster client.
   */
  XyncClusterClient undeployModule(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Asynchronously sets a key in the cluster.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return The cluster client.
   */
  XyncClusterClient set(String key, Object value);

  /**
   * Asynchronously sets a key in the cluster.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster client.
   */
  XyncClusterClient set(String key, Object value, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Asynchronously gets a key in the cluster.
   *
   * @param key The key to get.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster client.
   */
  <T> XyncClusterClient get(String key, Handler<AsyncResult<T>> resultHandler);

  /**
   * Asynchronously gets a key in the cluster.
   *
   * @param key The key to get.
   * @param def A default value to apply if the key does not exist.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster client.
   */
  <T> XyncClusterClient get(String key, Object def, Handler<AsyncResult<T>> resultHandler);

  /**
   * Asynchronously deletes a key from the cluster.
   *
   * @param key The key to delete.
   * @return The cluster client.
   */
  XyncClusterClient delete(String key);

  /**
   * Asynchronously deletes a key from the cluster.
   *
   * @param key The key to delete.
   * @param doneHandler An asynchronous handler to be called once complete.
   * @return The cluster client.
   */
  XyncClusterClient delete(String key, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Asynchronously checks if a key exists in the cluster.
   *
   * @param key The key to check.
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster client.
   */
  XyncClusterClient exists(String key, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Gets a list of available keys in the cluster.
   *
   * @param resultHandler An asynchronous handler to be called with the result.
   * @return The cluster client.
   */
  XyncClusterClient keys(Handler<AsyncResult<Set<String>>> resultHandler);

  /**
   * Watches a key in the cluster for all events.
   *
   * @param key The key to watch.
   * @param handler A handler to be called when an event occurs.
   * @return The cluster client.
   */
  XyncClusterClient watch(String key, Handler<Event> handler);

  /**
   * Watches a key in the cluster for a specific event.
   *
   * @param key The key to watch.
   * @param event The event to watch.
   * @param handler A handler to be called when an event occurs.
   * @return The cluster client.
   */
  XyncClusterClient watch(String key, Event.Type event, Handler<Event> handler);

  /**
   * Watches a key in the cluster for all events.
   *
   * @param key The key to watch.
   * @param handler A handler to be called when an event occurs.
   * @param doneHandler An asynchronous handler to be called once the watcher is registered
   *        in the cluster.
   * @return The cluster client.
   */
  XyncClusterClient watch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Watches a key in the cluster for a specific event.
   *
   * @param key The key to watch.
   * @param event The event to watch.
   * @param handler A handler to be called when an event occurs.
   * @param doneHandler An asynchronous handler to be called once the watcher is registered
   *        in the cluster.
   * @return The cluster client.
   */
  XyncClusterClient watch(String key, Event.Type event, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster for all events.
   *
   * @param key The key to unwatch.
   * @param handler The handler to unwatch.
   * @return The cluster client.
   */
  XyncClusterClient unwatch(String key, Handler<Event> handler);

  /**
   * Unwatches a key in the cluster for a specific event.
   *
   * @param key The key to unwatch.
   * @param event The event to unwatch.
   * @param handler The handler to unwatch.
   * @return The cluster client.
   */
  XyncClusterClient unwatch(String key, Event.Type event, Handler<Event> handler);

  /**
   * Unwatches a key in the cluster for all events.
   *
   * @param key The key to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the watcher is unregistered
   *        from the cluster.
   * @return The cluster client.
   */
  XyncClusterClient unwatch(String key, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Unwatches a key in the cluster for a specific event.
   *
   * @param key The key to unwatch.
   * @param event The event to unwatch.
   * @param handler The handler to unwatch.
   * @param doneHandler An asynchronous handler to be called once the watcher is unregistered
   *        from the cluster.
   * @return The cluster client.
   */
  XyncClusterClient unwatch(String key, Event.Type event, Handler<Event> handler, Handler<AsyncResult<Void>> doneHandler);

}
