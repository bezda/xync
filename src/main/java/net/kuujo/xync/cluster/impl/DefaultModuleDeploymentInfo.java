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

import net.kuujo.xync.cluster.DeploymentInfo;
import net.kuujo.xync.cluster.ModuleDeploymentInfo;

import org.vertx.java.core.json.JsonObject;

/**
 * Default module deployment info implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultModuleDeploymentInfo extends AbstractDeploymentInfo implements ModuleDeploymentInfo {
  private String module;

  private DefaultModuleDeploymentInfo() {
  }

  public DefaultModuleDeploymentInfo(JsonObject info) {
    id = info.getString("id");
    group = info.getString("group", "__DEFAULT__");
    config = info.containsField("config") ? info.getObject("config").toMap() : null;
    instances = info.getInteger("instances", 1);
    module = info.getString("module");
  }

  @Override
  public Type type() {
    return DeploymentInfo.Type.MODULE;
  }

  @Override
  public boolean isModule() {
    return true;
  }

  @Override
  public boolean isVerticle() {
    return false;
  }

  @Override
  public String module() {
    return module;
  }

  @Override
  public JsonObject toJson() {
    return new JsonObject()
        .putString("type", DeploymentInfo.Type.MODULE.getName())
        .putString("id", id)
        .putString("group", group)
        .putString("module", module)
        .putObject("config", config())
        .putNumber("instances", instances);
  }

  /**
   * A deployment info builder.
   * 
   * @author Jordan Halterman
   */
  public static class Builder {
    private DefaultModuleDeploymentInfo info;

    private Builder() {
      info = new DefaultModuleDeploymentInfo();
    }

    private Builder(DefaultModuleDeploymentInfo info) {
      this.info = info;
    }

    /**
     * Returns a new deployment info builder.
     * 
     * @return A new builder instance.
     */
    public static Builder newBuilder() {
      return new Builder();
    }

    /**
     * Returns a new deployment info builder.
     * 
     * @param info Base deployment info.
     * @return A new builder instance.
     */
    public static Builder newBuilder(DefaultModuleDeploymentInfo info) {
      return new Builder(info);
    }

    /**
     * Sets the deployment ID.
     * 
     * @param id The deployment ID.
     * @return The builder instance.
     */
    public Builder setId(String id) {
      info.id = id;
      return this;
    }

    /**
     * Sets the deployment node.
     *
     * @param node The deployment node.
     * @return The builder instance.
     */
    public Builder setNode(String node) {
      info.node = node;
      return this;
    }

    /**
     * Sets the deployment group.
     *
     * @param group The deployment group.
     * @return The builder instance.
     */
    public Builder setGroup(String group) {
      info.group = group;
      return this;
    }

    /**
     * Sets the deployment configuration.
     * 
     * @param config The deployment configuration.
     * @return The builder instance.
     */
    public Builder setConfig(JsonObject config) {
      info.config = config.toMap();
      return this;
    }

    /**
     * Sets the number of deployment instances.
     * 
     * @param instances The number of deployment instances.
     * @return The builder instance.
     */
    public Builder setInstances(int instances) {
      info.instances = instances;
      return this;
    }

    /**
     * Sets the deployment module.
     * 
     * @param moduleName The deployment module.
     * @return The builder instance.
     */
    public Builder setModule(String moduleName) {
      info.module = moduleName;
      return this;
    }

    /**
     * Builds the deployment info.
     * 
     * @return A new deployment info instance.
     */
    public ModuleDeploymentInfo build() {
      return info;
    }
  }

}
