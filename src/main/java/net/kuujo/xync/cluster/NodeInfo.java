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
package net.kuujo.xync.cluster;

import java.util.Collection;

import org.vertx.java.core.json.JsonObject;

/**
 * Node info.
 * 
 * @author Jordan Halterman
 */
public interface NodeInfo {

  /**
   * Returns the node ID.
   *
   * @return The unique node ID.
   */
  String id();

  /**
   * Returns the node HA group.
   *
   * @return The node HA group.
   */
  String group();

  /**
   * Returns a collection of deployments assigned to the node.
   * 
   * @return A collection of deployments assigned to the node.
   */
  Collection<DeploymentInfo> deployments();

  /**
   * Returns a json representation of the node info.
   *
   * @return Json node info.
   */
  JsonObject toJson();

}
