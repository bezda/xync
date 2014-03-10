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
package net.kuujo.zync.cluster;

import org.vertx.java.core.json.JsonObject;

/**
 * A cluster deployment.
 * 
 * @author Jordan Halterman
 */
public interface DeploymentInfo {

  /**
   * A deployment type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {

    /**
     * A module deployment type.
     */
    MODULE("module"),

    /**
     * A verticle deployment type.
     */
    VERTICLE("verticle");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    /**
     * Returns the deployment type name.
     * 
     * @return The deployment type name.
     */
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }

    /**
     * Parses a deployment type name to deployment type.
     *
     * @param name The deployment type name.
     * @return The deployment type.
     * @throws IllegalArgumentException If the deployment type is invalid.
     */
    public static Type parse(String name) {
      switch (name) {
        case "module":
          return MODULE;
        case "verticle":
          return VERTICLE;
        default:
          throw new IllegalArgumentException("Invalid deployment type " + name);
      }
    }

  }

  /**
   * Returns the deployment ID.
   * 
   * @return The deployment ID.
   */
  String id();

  /**
   * Returns the target deployment group.
   * 
   * @return The target deployment group.
   */
  String group();

  /**
   * Returns the deployment type.
   * 
   * @return The deployment type.
   */
  Type type();

  /**
   * Returns a boolean indicating whether the deployment is a module.
   * 
   * @return Indicates whether the deployment is a module.
   */
  boolean isModule();

  /**
   * Returns a boolean indicating whether the deployment is a verticle.
   * 
   * @return Indicates whether the deployment is a verticle.
   */
  boolean isVerticle();

  /**
   * Returns the deployment configuration.
   * 
   * @return The deployment configuration.
   */
  JsonObject config();

  /**
   * Returns the number of deployment instances.
   * 
   * @return The number of deployment instances.
   */
  int instances();

  /**
   * Returns the deployment info as module deployment info.
   *
   * @return Module deployment info.
   */
  ModuleDeploymentInfo asModule();

  /**
   * Returns the deployment info as verticle deployment info.
   *
   * @return Verticle deployment info.
   */
  VerticleDeploymentInfo asVerticle();

  /**
   * Returns a json representation of the deployment info.
   *
   * @return Json deployment info.
   */
  JsonObject toJson();

}
