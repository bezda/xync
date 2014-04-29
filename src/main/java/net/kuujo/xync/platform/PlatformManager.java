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
package net.kuujo.xync.platform;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;

/**
 * Xync platform manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PlatformManager {

  /**
   * Checks whether a deployment is deployed.
   *
   * @param deploymentID The deployment ID of the deployment to check.
   * @param resultHandler An asynchronous handler to be called once complete.
   */
  void isDeployed(String deploymentID, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Gets the current node assignment of a deployment.
   *
   * @param deploymentID The deployment ID of the deployment to check.
   * @param resultHandler An asynchronous handler to be called once complete. The
   *        handler will be called with the node ID of the node to which the deployment
   *        is assigned.
   */
  void getAssignment(String deploymentID, Handler<AsyncResult<String>> resultHandler);

  /**
   * Gets deployment info for a deployment.
   *
   * @param deploymentID The deployment ID of the deployment to get.
   * @param resultHandler An asynhronous handler to be called once complete.
   */
  void getDeploymentInfo(String deploymentID, Handler<AsyncResult<JsonObject>> resultHandler);

  /**
   * Deploys a module with a user-defined deployment ID.
   *
   * @param deploymentID The user defined deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param ha Indicates whether to deploy the deployment with HA.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployModuleAs(String deploymentID, String moduleName, JsonObject config, int instances, boolean ha, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle with a user-defined deployment ID.
   *
   * @param deploymentID The user defined deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param ha Indicates whether to deploy the deployment with HA.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployVerticleAs(String deploymentID, String main, JsonObject config, int instances, boolean ha, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle with a user-defined deployment ID.
   *
   * @param deploymentID The user defined deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param instances The number of instances to deploy.
   * @param multiThreaded Indicates whether the verticle is multi-threaded.
   * @param ha Indicates whether to deploy the deployment with HA.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployWorkerVerticleAs(String deploymentID, String main, JsonObject config, int instances, boolean multiThreaded, boolean ha, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a module with a user-defined deployment ID.
   *
   * @param deploymentID The user defined deployment ID.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void undeployModuleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a verticle with a user-defined deployment ID.
   *
   * @param deploymentID The user defined deployment ID.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void undeployVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Starts the platform manager.
   */
  void start();

  /**
   * Stops the platform manager.
   */
  void stop();

}
