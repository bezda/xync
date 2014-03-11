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

import java.util.Collection;
import java.util.HashSet;

import net.kuujo.xync.cluster.DeploymentInfo;
import net.kuujo.xync.cluster.NodeInfo;

import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

/**
 * Default node info implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultNodeInfo implements NodeInfo {
  private String id;
  private String group;
  private Collection<DeploymentInfo> deployments = new HashSet<>();

  private DefaultNodeInfo() {
  }

  public DefaultNodeInfo(JsonObject info) {
    id = info.getString("id");
    group = info.getString("group");
    JsonArray deployments = info.getArray("deployments");
    if (deployments != null) {
      for (Object deployment : deployments) {
        this.deployments.add(new DefaultModuleDeploymentInfo((JsonObject) deployment));
      }
    }
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public String group() {
    return group;
  }

  @Override
  public Collection<DeploymentInfo> deployments() {
    return deployments;
  }

  @Override
  public JsonObject toJson() {
    JsonArray deployments = new JsonArray();
    for (DeploymentInfo deployment : this.deployments) {
      deployments.add(deployment.toJson());
    }
    return new JsonObject()
        .putString("id", id)
        .putString("group", group)
        .putArray("deployments", deployments);
  }

  /**
   * Node info builder.
   * 
   * @author Jordan Halterman
   */
  public static class Builder {
    private final DefaultNodeInfo info;

    private Builder() {
      info = new DefaultNodeInfo();
    }

    private Builder(DefaultNodeInfo info) {
      this.info = info;
    }

    /**
     * Returns a new node info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new node info builder.
     * 
     * @param info Base node info.
     * @return A new builder instance.
     */
    public static Builder newBuilder(DefaultNodeInfo info) {
      return new Builder(info);
    }

    /**
     * Sets the node HA group.
     *
     * @param group The node HA group.
     * @return The builder instance.
     */
    public Builder setGroup(String group) {
      info.group = group;
      return this;
    }

    /**
     * Sets the node's deployments.
     * 
     * @param deployments A collection of deployment info.
     * @return The builder instance.
     */
    public Builder setDeployments(Collection<DeploymentInfo> deployments) {
      for (DeploymentInfo info : deployments) {
        addDeployment(info);
      }
      return this;
    }

    /**
     * Adds a deployment to the node.
     * 
     * @param info The deployment info.
     * @return The builder instance.
     */
    public Builder addDeployment(DeploymentInfo info) {
      this.info.deployments.add(info);
      return this;
    }

    /**
     * Removes a deployment from the node.
     * 
     * @param info The deployment info.
     * @return The builder instance.
     */
    public Builder removeDeployment(DeploymentInfo info) {
      this.info.deployments.remove(info);
      return this;
    }

    /**
     * Builds the node info.
     * 
     * @return A new node info instance.
     */
    public NodeInfo build() {
      return info;
    }
  }

}
