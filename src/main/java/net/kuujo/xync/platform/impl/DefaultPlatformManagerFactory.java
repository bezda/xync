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

import org.vertx.java.core.Vertx;
import org.vertx.java.platform.Container;

import net.kuujo.xync.cluster.ClusterManager;
import net.kuujo.xync.platform.PlatformManager;
import net.kuujo.xync.platform.PlatformManagerFactory;

/**
 * Default platform manager factory.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultPlatformManagerFactory implements PlatformManagerFactory {

  @Override
  public PlatformManager createPlatformManager(Vertx vertx, Container container, ClusterManager manager, int quorumSize, String cluster, String group, String node) {
    return new DefaultPlatformManager(vertx, container, manager, quorumSize, cluster, group, node);
  }

}
