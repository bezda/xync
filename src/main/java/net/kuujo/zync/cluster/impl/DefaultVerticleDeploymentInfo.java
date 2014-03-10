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

import net.kuujo.zync.cluster.DeploymentInfo;
import net.kuujo.zync.cluster.VerticleDeploymentInfo;
import net.kuujo.zync.cluster.WorkerVerticleDeploymentInfo;

import org.vertx.java.core.json.JsonObject;

/**
 * Default verticle deployment info implementation.
 *
 * @author Jordan Halterman
 */
public class DefaultVerticleDeploymentInfo extends AbstractDeploymentInfo implements VerticleDeploymentInfo {
  protected String main;

  protected DefaultVerticleDeploymentInfo() {
  }

  public DefaultVerticleDeploymentInfo(JsonObject info) {
    id = info.getString("id");
    group = info.getString("group", "__DEFAULT__");
    config = info.containsField("config") ? info.getObject("config").toMap() : null;
    instances = info.getInteger("instances", 1);
    main = info.getString("main");
  }

  @Override
  public Type type() {
    return Type.VERTICLE;
  }

  @Override
  public boolean isModule() {
    return false;
  }

  @Override
  public boolean isVerticle() {
    return true;
  }

  @Override
  public JsonObject toJson() {
    return new JsonObject()
        .putString("type", DeploymentInfo.Type.VERTICLE.getName())
        .putString("id", id)
        .putString("group", group)
        .putString("main", main)
        .putBoolean("worker", false)
        .putObject("config", config())
        .putNumber("instances", instances);
  }

  @Override
  public String main() {
    return main;
  }

  @Override
  public boolean isWorker() {
    return false;
  }

  @Override
  public WorkerVerticleDeploymentInfo asWorker() {
    return (WorkerVerticleDeploymentInfo) this;
  }

  /**
   * A deployment info builder.
   * 
   * @author Jordan Halterman
   */
  public static class Builder {
    private DefaultVerticleDeploymentInfo info;

    private Builder() {
      info = new DefaultVerticleDeploymentInfo();
    }

    private Builder(DefaultVerticleDeploymentInfo info) {
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
    public static Builder newBuilder(DefaultVerticleDeploymentInfo info) {
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
     * Sets the deployment main.
     * 
     * @param main The deployment main.
     * @return The builder instance.
     */
    public Builder setMain(String main) {
      info.main = main;
      return this;
    }

    /**
     * Builds the deployment info.
     * 
     * @return A new deployment info instance.
     */
    public VerticleDeploymentInfo build() {
      return info;
    }
  }

}
