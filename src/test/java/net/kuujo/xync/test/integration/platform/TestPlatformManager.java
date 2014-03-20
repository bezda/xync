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

import java.util.Map;

import net.kuujo.xync.platform.impl.DefaultXyncPlatformManager;

import org.vertx.java.core.Handler;
import org.vertx.java.platform.impl.Deployment;

public class TestPlatformManager extends DefaultXyncPlatformManager {

  public TestPlatformManager(int port, String hostname, int quorumSize, String haGroup) {
    super(port, hostname, quorumSize, haGroup);
  }

  void failDuringFailover(boolean fail) {
    xyncHaManager.failDuringFailover(fail);
  }

  public void simulateKill() {
    if (xyncHaManager != null) {
      xyncHaManager.simulateKill();
    }
    super.stop();
  }

  // For testing only
  public Map<String, Deployment> getDeployments() {
    return deployments;
  }

  public void failoverCompleteHandler(Handler<Boolean> handler) {
    if (xyncHaManager != null) {
      xyncHaManager.failoverCompleteHandler(handler);
    }
  }
}