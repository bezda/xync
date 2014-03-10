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
  private String cluster;
  private String address;
  private Collection<DeploymentInfo> deployments = new HashSet<>();

  private DefaultNodeInfo() {
  }

  public DefaultNodeInfo(JsonObject info) {
    id = info.getString("id");
    cluster = info.getString("cluster");
    address = info.getString("address");
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
  public String cluster() {
    return cluster;
  }

  @Override
  public String address() {
    return address;
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
        .putString("cluster", cluster)
        .putString("address", address)
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
     * Sets the node cluster.
     *
     * @param cluster The node cluster.
     * @return The builder instance.
     */
    public Builder setCluster(String cluster) {
      info.cluster = cluster;
      return this;
    }

    /**
     * Sets the node address.
     * 
     * @param address The node address.
     * @return The builder instance.
     */
    public Builder setAddress(String address) {
      info.address = address;
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
