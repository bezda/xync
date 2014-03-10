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
package net.kuujo.zync.cluster.impl;

import net.kuujo.zync.cluster.ZyncClusterManager;
import net.kuujo.zync.cluster.ZyncClusterService;
import net.kuujo.zync.cluster.ZyncClusterServiceFactory;

/**
 * Default service factory implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultZyncClusterServiceFactory implements ZyncClusterServiceFactory {

  @Override
  public ZyncClusterService createClusterService(ZyncClusterManager clusterManager) {
    return new DefaultZyncClusterService(clusterManager);
  }

}
