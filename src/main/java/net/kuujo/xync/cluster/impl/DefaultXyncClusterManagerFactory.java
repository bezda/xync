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
package net.kuujo.xync.cluster.impl;

import net.kuujo.xync.cluster.XyncClusterManager;
import net.kuujo.xync.cluster.XyncClusterManagerFactory;

import org.vertx.java.core.impl.VertxInternal;
import org.vertx.java.core.spi.cluster.ClusterManager;
import org.vertx.java.platform.impl.PlatformManagerInternal;

/**
 * Default Xing cluster manger factory.
 *
 * @author Jordan Halterman
 */
public class DefaultXyncClusterManagerFactory implements XyncClusterManagerFactory {

  @Override
  public XyncClusterManager createClusterManager(String group, VertxInternal vertx, PlatformManagerInternal platformManager, ClusterManager clusterManager) {
    return new DefaultXyncClusterManager(group, vertx, platformManager, clusterManager);
  }

}
