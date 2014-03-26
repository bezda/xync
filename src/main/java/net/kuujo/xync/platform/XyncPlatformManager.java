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

import java.net.URL;

import net.kuujo.xync.cluster.DeploymentInfo;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.impl.PlatformManagerInternal;

/**
 * Xync platform manager.
 *
 * @author Jordan Halterman
 */
public interface XyncPlatformManager extends PlatformManagerInternal {

  /**
   * Checks if a deployment with the given deployment ID exists.
   *
   * @param deploymentID The deployment ID to check.
   * @param resultHandler An asynchronous result handler.
   */
  void isDeployedAs(String deploymentID, Handler<AsyncResult<Boolean>> resultHandler);

  /**
   * Returns deployment info.
   *
   * @param deploymentID The deployment ID.
   * @param resultHandler An asynchronous handler to be called with the deployment info.
   * @return The platform manager.
   */
  void getDeploymentInfo(String deploymentID, Handler<AsyncResult<DeploymentInfo>> resultHandler);

  /**
   * Deploys a module.
   *
   * @param deploymentID The module deployment ID.
   * @param moduleName The module name.
   * @param config The module configuration.
   * @param instances The number of instances to deploy.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployModuleAs(String deploymentID, String moduleName, JsonObject config, int instances, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a verticle.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param includes An includes string.
   * @param ha Indicates whether to deploy with high-availability.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, String includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Deploys a worker verticle.
   *
   * @param deploymentID The verticle deployment ID.
   * @param main The verticle main.
   * @param config The verticle configuration.
   * @param classpath The verticle classpath.
   * @param instances The number of instances to deploy.
   * @param includes An includes string.
   * @param ha Indicates whether to deploy with high-availability.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void deployWorkerVerticleAs(String deploymentID, String main, JsonObject config, URL[] classpath, int instances, boolean multiThreaded, String includes, Handler<AsyncResult<String>> doneHandler);

  /**
   * Undeploys a module.
   *
   * @param deploymentID The module deployment ID.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void undeployModuleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

  /**
   * Undeploys a verticle.
   *
   * @param deploymentID The verticle deployment ID.
   * @param doneHandler An asynchronous handler to be called once complete.
   */
  void undeployVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler);

}
