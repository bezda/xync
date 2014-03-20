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

import java.util.UUID;

import net.kuujo.xync.platform.XyncPlatformManager;
import net.kuujo.xync.platform.XyncPlatformManagerFactory;

import org.vertx.java.platform.PlatformManager;

/**
 * Xing platform manager factory.
 *
 * @author Jordan Halterman
 */
public class DefaultXyncPlatformManagerFactory implements XyncPlatformManagerFactory {

  @Override
  public PlatformManager createPlatformManager() {
    throw new UnsupportedOperationException("Xync platform manager must be run in cluster mode.");
  }

  @Override
  public PlatformManager createPlatformManager(int clusterPort, String clusterHost) {
    return new DefaultXyncPlatformManager(clusterPort, clusterHost, 1, UUID.randomUUID().toString());
  }

  @Override
  public XyncPlatformManager createPlatformManager(int clusterPort, String clusterHost, int quorumSize, String group) {
    return new DefaultXyncPlatformManager(clusterPort, clusterHost, quorumSize, group);
  }

}
