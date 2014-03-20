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

import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.xync.cluster.XyncClusterManager;
import net.kuujo.xync.cluster.XyncClusterService;
import net.kuujo.xync.cluster.impl.DefaultXyncClusterManagerFactory;
import net.kuujo.xync.cluster.impl.DefaultXyncClusterServiceFactory;
import net.kuujo.xync.platform.XyncPlatformManager;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.impl.DefaultPlatformManager;

/**
 * Xing platform manager.
 *
 * @author Jordan Halterman
 */
public class DefaultXyncPlatformManager extends DefaultPlatformManager implements XyncPlatformManager {
  protected final XyncHAManager xyncHaManager;
  protected final XyncClusterManager xyncManager;
  protected final XyncClusterService xyncService;
  private final Vertx vertx;

  public DefaultXyncPlatformManager(int port, String hostname, int quorumSize, String group) {
    super(port, hostname, quorumSize, group);
    this.vertx = vertx();
    this.xyncManager = new DefaultXyncClusterManagerFactory().createClusterManager(group, (VertxInternal) vertx, this, clusterManager);
    this.xyncService = new DefaultXyncClusterServiceFactory().createClusterService(xyncManager);
    this.xyncHaManager = new XyncHAManager((VertxInternal) vertx, this, clusterManager, quorumSize, group);
    startCluster();
    startService();
  }

  /**
   * Starts the Xing cluster manager.
   */
  private void startCluster() {
    final CountDownLatch latch = new CountDownLatch(1);
    xyncManager.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        latch.countDown();
      }
    });
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Starts the Xing cluster service.
   */
  private void startService() {
    final CountDownLatch latch = new CountDownLatch(1);
    xyncService.start(new Handler<AsyncResult<Void>>() {
      @Override
      public void handle(AsyncResult<Void> result) {
        latch.countDown();
      }
    });
    try {
      latch.await(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized void deployModuleAs(final String deploymentID, final String moduleName, final JsonObject config, final int instances, final Handler<AsyncResult<String>> doneHandler) {
    if (xyncHaManager != null) {
      xyncHaManager.deployModuleAs(deploymentID, moduleName, config, instances, doneHandler);
    }
    else {
      deployModule(moduleName, config, instances, doneHandler);
    }
  }

  @Override
  public synchronized void deployVerticleAs(final String deploymentID, final String main, final JsonObject config, final URL[] classpath, final int instances,
      final String includes, final Handler<AsyncResult<String>> doneHandler) {
    if (xyncHaManager != null) {
      xyncHaManager.deployVerticleAs(deploymentID, main, config, classpath, instances, includes, doneHandler);
    }
    else {
      deployVerticle(main, config, classpath, instances, includes, doneHandler);
    }
  }

  @Override
  public synchronized void deployWorkerVerticleAs(final String deploymentID, final String main, final JsonObject config, final URL[] classpath, final int instances,
      final boolean multiThreaded, final String includes, final Handler<AsyncResult<String>> doneHandler) {
    if (xyncHaManager != null) {
      xyncHaManager.deployWorkerVerticleAs(deploymentID, main, config, classpath, instances, multiThreaded, includes, doneHandler);
    }
    else {
      deployWorkerVerticle(multiThreaded, main, config, classpath, instances, includes, doneHandler);
    }
  }

  @Override
  public void undeployModuleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    if (xyncHaManager != null) {
      xyncHaManager.undeployModuleAs(deploymentID, doneHandler);
    }
  }

  @Override
  public void undeployVerticleAs(String deploymentID, Handler<AsyncResult<Void>> doneHandler) {
    if (xyncHaManager != null) {
      xyncHaManager.undeployVerticleAs(deploymentID, doneHandler);
    }
  }

  @Override
  public void stop() {
    if (xyncHaManager != null) {
      xyncHaManager.stop();
    }
    super.stop();
  }

}
