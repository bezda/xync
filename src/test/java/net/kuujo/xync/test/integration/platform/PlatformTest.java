/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package net.kuujo.xync.test.integration.platform;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;
import net.kuujo.xync.test.integration.cluster.FakeClusterManager;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;
import org.vertx.java.platform.impl.Deployment;

/**
 * Taken mostly from Vert.x core.
 */
public class PlatformTest extends TestCase {

  public static class TestVerticle1 extends Verticle {
    @Override
    public void start() {
      super.start();
    }
  }

  public static class TestVerticle2 extends Verticle {
    @Override
    public void start() {
      super.start();
    }
  }

  protected void setUp() throws Exception {
    super.setUp();
    System.setProperty("vertx.mods", "src/test/mod-test");
    //System.setProperty("vertx.mods", "vertx-testsuite/src/test/mod-test");
    System.setProperty("vertx.clusterManagerFactory", "net.kuujo.xync.test.integration.cluster.FakeClusterManagerFactory");
  }

  protected void tearDown() throws Exception {
    FakeClusterManager.reset();
    super.tearDown();
  }

  private Random random = new Random();

  public void testSimpleModuleNode0() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleVerticleNode0() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleWorkerNode0() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleModuleNode1() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleVerticleNode1() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleWorkerNode1() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleModuleNode2() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(2);
    cluster.closeCluster();
  }

  public void testSimpleVerticleNode2() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(2);
    cluster.closeCluster();
  }

  public void testSimpleWorkerNode2() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())));
    cluster.killNode(2);
    cluster.closeCluster();
  }

  public void testMultipleModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleWorker() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.WORKER, TestVerticle2.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleMixed() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleSameModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleSameVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleSameWorker() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.WORKER, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleModuleUndeploy() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    NodeMods mods = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployMods(1, mods);
    cluster.undeployMods(1, mods);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleVerticleUndeploy() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    NodeMods mods = new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle2.class.getName()));
    cluster.deployMods(1, mods);
    cluster.undeployMods(1, mods);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleWorkerUndeploy() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    NodeMods mods = new NodeMods().addDeployment(new DepInfo("1", Type.WORKER, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.WORKER, TestVerticle2.class.getName()));
    cluster.deployMods(1, mods);
    cluster.undeployMods(1, mods);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testLots() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    NodeMods mods = new NodeMods();
    for (int i = 0; i < 50; i++) {
      mods.addDeployment(new DepInfo(UUID.randomUUID().toString(), Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo(UUID.randomUUID().toString(), Type.MODULE, "net.kuujo~xync-test2~1.0"));
    }
    for (int i = 0; i < 50; i++) {
      mods.addDeployment(new DepInfo(UUID.randomUUID().toString(), Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo(UUID.randomUUID().toString(), Type.VERTICLE, TestVerticle2.class.getName()));
    }
    cluster.deployMods(1, mods);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesSameModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesSameVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesDifferentModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleOtherNodesDifferentVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

//  public void testLoop() throws Exception {
//    int iters = 10000000;
//    for (int i = 0; i < iters; i++) {
//      System.out.println("****************************** ITER " + i);
//      testMultipleOtherNodesDifferentMod();
//      tearDown();
//      if (i != iters - 1) {
//        setUp();
//      }
//    }
//  }

  public void testMultipleOtherNodesDifferentModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleOtherNodesDifferentVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("4", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleOtherNodesSameModule() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("5", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("6", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testMultipleOtherNodesSameVerticle() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("4", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("5", Type.VERTICLE, TestVerticle1.class.getName())).addDeployment(new DepInfo("6", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testKillAllNoDeployments() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.killNode(1);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleMultipleKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    killAllClusterSequentially(cluster, clusterSize);
    cluster.closeCluster();
  }

  private void killAllClusterSequentially(Cluster cluster, int clusterSize) throws Exception {
    int pos = 0;
    for (int size = clusterSize; size > 1; size--) {
      cluster.killNode(pos);
      pos++;
      if (pos >= size - 1) {
        pos = 0;
      }
    }
  }

  public void testMultipleMultipleKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    killAllClusterSequentially(cluster, clusterSize);
    cluster.closeCluster();
  }

  private void killAllRandomly(Cluster cluster, int clusterSize) throws Exception {
    for (int size = clusterSize; size > 1; size--) {
      int toKill = random.nextInt(size);
      cluster.killNode(toKill);
    }
  }

  public void testSimpleRandomKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    killAllRandomly(cluster, clusterSize);
    cluster.closeCluster();
  }

  public void testMultipleRandomKill() throws Exception {
    int clusterSize = 10;
    Cluster cluster = new Cluster(clusterSize);
    cluster.createCluster();
    cluster.deployMods(clusterSize / 2, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    killAllRandomly(cluster, clusterSize);
    cluster.closeCluster();
  }

  public void testSimpleKillAndDeployMoreModules() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.killNode(1);
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("5", Type.MODULE, "net.kuujo~xync-test2~1.0")));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleKillAndDeployMoreVerticles() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.killNode(1);
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("4", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("5", Type.VERTICLE, TestVerticle2.class.getName())));
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testCloseThenKillModules() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.closeNode(0);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testCloseThenKillVerticles() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.VERTICLE, TestVerticle1.class.getName())));
    cluster.closeNode(0);
    cluster.killNode(1);
    cluster.closeCluster();
  }

  public void testSimpleNodeConfigAndInstances() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0", 3, new JsonObject().putString("foo", "bar"))));
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testSimpleNodeConfigAndInstancesSameModuleOtherNodes() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0", 6, new JsonObject().putString("foo", "bar"))));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0", 3, new JsonObject().putString("blah", "eek"))));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0", 7, new JsonObject().putString("foo", "quux"))));
    cluster.killNode(0);
    cluster.killNode(0);
    cluster.closeCluster();
  }

  public void testGroups() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1"));
    cluster.nodes.add(new NodeMods("group1"));
    cluster.nodes.add(new NodeMods("group2"));
    cluster.nodes.add(new NodeMods("group2"));
    cluster.nodes.add(new NodeMods("group3"));
    cluster.nodes.add(new NodeMods("group3"));

    cluster.createCluster();
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(1, new NodeMods().addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(2, new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(3, new NodeMods().addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(4, new NodeMods().addDeployment(new DepInfo("5", Type.MODULE, "net.kuujo~xync-test1~1.0")));
    cluster.deployMods(5, new NodeMods().addDeployment(new DepInfo("6", Type.MODULE, "net.kuujo~xync-test1~1.0")));

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    assertEquals(0, cluster.killNode(0));
    cluster.closeNode(0);

    cluster.closeCluster();
  }

  public void testFailureDuringFailover() throws Exception {
    Cluster cluster = new Cluster(3);
    cluster.createCluster();
    cluster.pms.get(1).failDuringFailover(true);
    cluster.pms.get(2).failDuringFailover(true);
    cluster.deployMods(0, new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0", 6, new JsonObject().putString("foo", "bar"))));

    // -1 means failover failed
    assertEquals(-1, cluster.killNode(0));

    // Next time we won't fail during failover - the next failover should take over the previous failed node which
    // was in limbo
    cluster.pms.get(1).failDuringFailover(false);
    assertEquals(0, cluster.killNode(0));

    cluster.closeCluster();
  }

  public void testSimpleQuorum() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 2));
    cluster.createCluster();

    NodeMods mods = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0"));
    cluster.deployModsNoCheck(0, mods);

    Thread.sleep(500);
    // Make sure it doesn't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 2));

    // Should now be deployed
    cluster.checkModulesDeployed(0, mods);

    cluster.closeCluster();
  }

  public void testSimpleQuorumLastAddedHasDeployments() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 2));
    cluster.createCluster();

    NodeMods mods0 = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0"));
    cluster.deployModsNoCheck(0, mods0);

    Thread.sleep(500);
    // Make sure it doesn't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 2));
    NodeMods mods1 = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployMods(1, mods1);

    // Mods on 0 should now be deployed
    cluster.checkModulesDeployed(0, mods0);

    cluster.closeCluster();
  }

  public void testQuorumMultipleSameNode() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 2));
    cluster.createCluster();

    NodeMods mods = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployModsNoCheck(0, mods);

    Thread.sleep(500);
    // Make sure it doesn't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 2));

    // Should now be deployed
    cluster.checkModulesDeployed(0, mods);

    cluster.closeCluster();
  }

  public void testQuorumSeveralNodes() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 4));
    cluster.nodes.add(new NodeMods("group1", 4));
    cluster.nodes.add(new NodeMods("group1", 4));
    cluster.createCluster();

    NodeMods mods0 = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    NodeMods mods1 = new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    NodeMods mods2 = new NodeMods().addDeployment(new DepInfo("5", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("6", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployModsNoCheck(0, mods0);
    cluster.deployModsNoCheck(1, mods1);
    cluster.deployModsNoCheck(2, mods2);

    Thread.sleep(500);
    // Make sure they don't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());
    assertTrue(cluster.pms.get(1).getDeployments().isEmpty());
    assertTrue(cluster.pms.get(2).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 4));

    // Should now be deployed
    cluster.checkModulesDeployed(0, mods0);
    cluster.checkModulesDeployed(1, mods1);
    cluster.checkModulesDeployed(2, mods2);

    cluster.closeCluster();
  }

  public void testQuorumNoQuorum() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 2));
    cluster.createCluster();

    NodeMods mods0 = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployModsNoCheck(0, mods0);

    Thread.sleep(500);
    // Make sure they don't deploy yet - we don't have a quorum
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 2));

    // Should now be deployed
    cluster.checkModulesDeployed(0, mods0);

    //Now close node
    cluster.closeNode(1);

    // Now we have no quorum

    cluster.checkNoModulesDeployed(0);

    //Now add another node
    cluster.addNode(new NodeMods("group1", 2));

    // Should now be deployed again
    cluster.checkModulesDeployedStill(0);

    cluster.closeCluster();
  }

  public void testQuorumWithGroups() throws Exception {
    Cluster cluster = new Cluster();
    cluster.nodes.add(new NodeMods("group1", 3));
    cluster.nodes.add(new NodeMods("group1", 3));
    cluster.nodes.add(new NodeMods("group2", 1));
    cluster.createCluster();

    NodeMods mods0 = new NodeMods().addDeployment(new DepInfo("1", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("2", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    NodeMods mods1 = new NodeMods().addDeployment(new DepInfo("3", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("4", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    NodeMods mods2 = new NodeMods().addDeployment(new DepInfo("5", Type.MODULE, "net.kuujo~xync-test1~1.0")).addDeployment(new DepInfo("6", Type.MODULE, "net.kuujo~xync-test2~1.0"));
    cluster.deployModsNoCheck(0, mods0);
    cluster.deployModsNoCheck(1, mods1);
    cluster.deployModsNoCheck(2, mods2);

    Thread.sleep(500);
    assertTrue(cluster.pms.get(0).getDeployments().isEmpty());
    assertTrue(cluster.pms.get(1).getDeployments().isEmpty());
    //Should be deployed on node 2
    cluster.checkModulesDeployed(2, mods2);

    // Now deploy another node
    cluster.addNode(new NodeMods("group1", 3));

    // Should now be deployed on all nodes
    cluster.checkModulesDeployed(0, mods0);
    cluster.checkModulesDeployed(1, mods1);
    cluster.checkModulesDeployedStill(2);

    cluster.closeCluster();
  }

  class Cluster {
    List<NodeMods> nodes = new ArrayList<>();
    List<TestPlatformManager> pms;

    Cluster(int nodeCount) {
      for (int i = 0; i < nodeCount; i++) {
        nodes.add(new NodeMods());
      }
    }

    Cluster() {
    }

    void createCluster() {
      pms = new ArrayList<>();
      for (NodeMods node: nodes) {
        pms.add(new TestPlatformManager(0, "localhost", node.quorumSize, node.group));
      }
    }

    void addNode(NodeMods node) {
      nodes.add(node);
      pms.add(new TestPlatformManager(0, "localhost", node.quorumSize, node.group));
    }

    void deployMods(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      final CountDownLatch latch = new CountDownLatch(nodeMods.deployments.size());
      for (DepInfo dep: nodeMods.deployments) {
        if (dep.type.equals(Type.MODULE)) {
          pm.deployModuleAs(dep.id, dep.modName, dep.config, dep.instances, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (res.succeeded()) {
                latch.countDown();
              } else {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        } else if (dep.type.equals(Type.VERTICLE)) {
          pm.deployVerticleAs(dep.id, dep.modName, dep.config, new URL[]{}, dep.instances, null, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (res.succeeded()) {
                latch.countDown();
              } else {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        } else if (dep.type.equals(Type.WORKER)) {
          pm.deployWorkerVerticleAs(dep.id, dep.modName, dep.config, new URL[]{}, dep.instances, false, null, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (res.succeeded()) {
                latch.countDown();
              } else {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        }
      }
      assertTrue(latch.await(120, TimeUnit.SECONDS));
      checkModulesDeployed(node, nodeMods);
    }

    void deployModsNoCheck(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      for (DepInfo dep: nodeMods.deployments) {
        if (dep.type.equals(Type.MODULE)) {
          pm.deployModuleAs(dep.id, dep.modName, dep.config, dep.instances, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (!res.succeeded()) {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        } else if (dep.type.equals(Type.VERTICLE)) {
          pm.deployVerticleAs(dep.id, dep.modName, dep.config, new URL[]{}, dep.instances, null, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (!res.succeeded()) {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        } else if (dep.type.equals(Type.WORKER)) {
          pm.deployWorkerVerticleAs(dep.id, dep.modName, dep.config, new URL[]{}, dep.instances, false, null, new Handler<AsyncResult<String>>() {
            @Override
            public void handle(AsyncResult<String> res) {
              if (!res.succeeded()) {
                res.cause().printStackTrace();
                fail("Failed to deploy module");
              }
            }
          });
        }
      }
    }

    void checkModulesDeployed(int node, NodeMods nodeMods) throws Exception {
      NodeMods existingMods = nodes.get(node);
      existingMods.deployments.addAll(nodeMods.deployments);
      NodeMods mods = nodes.get(node);
      TestPlatformManager pm = pms.get(node);
      long start = System.currentTimeMillis();
      outer: while (true) {
        Thread.sleep(1);
        if (System.currentTimeMillis() - start > 10000) {
          throw new IllegalStateException("Timed out waiting for deployments");
        }
        if (mods.deployments.size() != pm.getDeployments().size()) {
          continue;
        }
        for (DepInfo dep: mods.deployments) {
          if (dep.type.equals(Type.MODULE)) {
            if (!hasModule(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          } else {
            if (!hasVerticle(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          }
        }
        break;
      }
    }

    void checkModulesDeployedStill(int node) throws Exception {
      NodeMods mods = nodes.get(node);
      TestPlatformManager pm = pms.get(node);
      long start = System.currentTimeMillis();
      outer: while (true) {
        Thread.sleep(1);
        if (System.currentTimeMillis() - start > 10000) {
          throw new IllegalStateException("Timed out waiting for deployments");
        }
        if (mods.deployments.size() != pm.getDeployments().size()) {
          continue;
        }
        for (DepInfo dep: mods.deployments) {
          if (dep.type.equals(Type.MODULE)) {
            if (!hasModule(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          } else {
            if (!hasVerticle(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          }
        }
        break;
      }
    }

    void checkNoModulesDeployed(int node) throws Exception {
      TestPlatformManager pm = pms.get(node);
      long start = System.currentTimeMillis();
      outer: while (true) {
        Thread.sleep(1);
        if (System.currentTimeMillis() - start > 10000) {
          throw new IllegalStateException("Timed out waiting for deployments");
        }
        if (!pm.getDeployments().isEmpty()) {
          continue;
        }
        break;
      }
    }

    void undeployMods(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      final CountDownLatch latch = new CountDownLatch(nodeMods.deployments.size());
      for (DepInfo dep : nodeMods.deployments) {
        if (dep.type.equals(Type.MODULE)) {
          pm.undeployModuleAs(dep.id, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.succeeded()) {
                latch.countDown();
              } else {
                result.cause().printStackTrace();
                fail("Failed to undeploy module.");
              }
            }
          });
        } else if (dep.type.equals(Type.VERTICLE) || dep.type.equals(Type.WORKER)) {
          pm.undeployVerticleAs(dep.id, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (result.succeeded()) {
                latch.countDown();
              } else {
                result.cause().printStackTrace();
                fail("Failed to undeploy module.");
              }
            }
          });
        }
      }
      assertTrue(latch.await(120, TimeUnit.SECONDS));
      checkModulesUndeployed(node, nodeMods);
    }

    void undeployModsNoCheck(int node, NodeMods nodeMods) throws Exception {
      TestPlatformManager pm = pms.get(node);
      for (DepInfo dep: nodeMods.deployments) {
        if (dep.type.equals(Type.MODULE)) {
          pm.undeployModuleAs(dep.id, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (!result.succeeded()) {
                result.cause().printStackTrace();
                fail("Failed to undeploy module.");
              }
            }
          });
        } else if (dep.type.equals(Type.VERTICLE) || dep.type.equals(Type.WORKER)) {
          pm.undeployVerticleAs(dep.id, new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> result) {
              if (!result.succeeded()) {
                result.cause().printStackTrace();
                fail("Failed to undeploy module.");
              }
            }
          });
        }
      }
    }

    void checkModulesUndeployed(int node, NodeMods nodeMods) throws Exception {
      NodeMods existingMods = nodes.get(node);
      existingMods.deployments.removeAll(nodeMods.deployments);
      TestPlatformManager pm = pms.get(node);
      long start = System.currentTimeMillis();
      outer: while (true) {
        Thread.sleep(1);
        if (System.currentTimeMillis() - start > 10000) {
          throw new IllegalStateException("Timed out waiting for undeployments");
        }
        for (DepInfo dep: nodeMods.deployments) {
          if (dep.type.equals(Type.MODULE)) {
            if (hasModule(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          } else {
            if (hasVerticle(dep.modName, pm.getDeployments())) {
              continue outer;
            }
          }
        }
        break;
      }
    }

    void checkNoModulesUndeployed(int node) throws Exception {
      TestPlatformManager pm = pms.get(node);
      long start = System.currentTimeMillis();
      outer: while (true) {
        Thread.sleep(1);
        if (System.currentTimeMillis() - start > 10000) {
          throw new IllegalStateException("Timed out waiting for undeployments");
        }
        if (pm.getDeployments().isEmpty()) {
          break;
        }
      }
    }

    void closeCluster() {
      for (TestPlatformManager pm: pms) {
        pm.stop();
      }
    }

    void closeNode(int node) {
      pms.get(node).stop();
      nodes.remove(node);
      pms.remove(node);
    }

    NodeMods limboMods;

    int killNode(int node) throws Exception {
      TestPlatformManager toKill = pms.get(node);
      NodeMods failoverMods = nodes.get(node);
      nodes.remove(node);
      pms.remove(node);

      final CountDownLatch failoverLatch = new CountDownLatch(1);
      final AtomicInteger afailoverNode = new AtomicInteger(-1);
      for (int i = 0; i < pms.size(); i++) {
        final int nodeID = i;
        pms.get(i).failoverCompleteHandler(new Handler<Boolean>() {
          @Override
          public void handle(Boolean b) {
            if (b) {
              afailoverNode.set(nodeID);
            }
            failoverLatch.countDown();
          }
        });
      }

      toKill.simulateKill();

      assertTrue(failoverLatch.await(120, TimeUnit.SECONDS));

      int failoverNode = afailoverNode.get();

      if (failoverNode != -1) {
        // Now make sure that the mods on nodes other than the kill node are still there
        for (int i = 0; i < pms.size(); i++) {
          Map<String, Deployment> deployments = pms.get(i).getDeployments();
          Collection<Deployment> deps = deployments.values();
          NodeMods expectedMods = nodes.get(i);
          if (i == failoverNode) {
            expectedMods.deployments.addAll(failoverMods.deployments);
            // Also add in any mods which are in limbo from a previous failed failover attempt
            if (limboMods != null) {
              expectedMods.deployments.addAll(limboMods.deployments);
              limboMods = null;
            }
          }
          for (DepInfo expectedDep: expectedMods.deployments) {
            assertTrue(containsDep(deps, expectedDep));
          }
        }
      } else {
        limboMods = failoverMods;
      }
      return failoverNode;
    }

    private boolean containsDep(Collection<Deployment> deps, DepInfo dep) {
      for (Deployment d: deps) {
        if (dep.type.equals(Type.MODULE)) {
          if (d.modID != null && !d.modID.toString().equals(dep.modName))  continue;
        } else {
          if (!d.main.equals(dep.modName)) continue;
        }
        if (d.config == null && dep.config != null) continue;
        if (d.config != null && !d.config.equals(dep.config)) continue;
        if (d.instances != dep.instances) continue;
        return true;
      }
      return false;
    }

    private boolean hasModule(String moduleName, Map<String, Deployment> deployments) {
      for (Deployment dep: deployments.values()) {
        if (dep.modID != null && dep.modID.toString().equals(moduleName)) {
          return true;
        }
      }
      return false;
    }

    private boolean hasVerticle(String main, Map<String, Deployment> deployments) {
      for (Deployment dep : deployments.values()) {
        if (dep.main != null && dep.main.equals(main)) {
          return true;
        }
      }
      return false;
    }

  }

  class NodeMods {
    List<DepInfo> deployments = new ArrayList<>();
    String group;
    int quorumSize;

    NodeMods() {
      this(null);
    }

    NodeMods(String group) {
      this.group = group;
    }

    NodeMods(String group, int quorumSize) {
      this.group = group;
      this.quorumSize = quorumSize;
    }

    NodeMods addDeployment(DepInfo dep) {
      this.deployments.add(dep);
      return this;
    }
  }

  static enum Type {
    MODULE,
    VERTICLE,
    WORKER;
  }

  class DepInfo {
    String id;
    Type type;
    String modName;
    int instances;
    JsonObject config;

    DepInfo(String deploymentID, Type type, String modName, int instances, JsonObject config) {
      this.id = deploymentID;
      this.type = type;
      this.modName = modName;
      this.instances = instances;
      this.config = config;
    }

    DepInfo(String deploymentID, Type type, String modName) {
      this(deploymentID, type, modName, 1, new JsonObject());
    }
  }

}
