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
package net.kuujo.zync.platform.impl;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import net.kuujo.zync.cluster.ZyncClusterManager;
import net.kuujo.zync.cluster.ZyncClusterService;
import net.kuujo.zync.cluster.impl.DefaultZyncClusterManagerFactory;
import net.kuujo.zync.cluster.impl.DefaultZyncClusterServiceFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.platform.impl.DefaultPlatformManager;
import org.vertx.java.platform.impl.PlatformManagerInternal;

/**
 * Xing platform manager.
 *
 * @author Jordan Halterman
 */
public class ZyncPlatformManager extends DefaultPlatformManager {
  protected final ZyncClusterManager zyncManager;
  protected final ZyncClusterService zyncService;
  private final Vertx vertx;

  public ZyncPlatformManager(int port, String hostname, int quorumSize, String group) {
    super(port, hostname, quorumSize, group);
    this.vertx = vertx();
    this.zyncManager = new DefaultZyncClusterManagerFactory().createClusterManager(group, (VertxInternal) vertx, this, clusterManager);
    this.zyncService = new DefaultZyncClusterServiceFactory().createClusterService(zyncManager);
    this.haManager = new ZyncHAManager((VertxInternal) vertx, (PlatformManagerInternal) this, clusterManager, quorumSize, group);
    startCluster();
    startService();
  }

  /**
   * Starts the Xing cluster manager.
   */
  private void startCluster() {
    final CountDownLatch latch = new CountDownLatch(1);
    zyncManager.start(new Handler<AsyncResult<Void>>() {
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
    zyncService.start(new Handler<AsyncResult<Void>>() {
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

}
