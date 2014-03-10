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

import java.util.Map;

import org.vertx.java.core.json.JsonObject;

import net.kuujo.zync.cluster.DeploymentInfo;
import net.kuujo.zync.cluster.ModuleDeploymentInfo;
import net.kuujo.zync.cluster.VerticleDeploymentInfo;

/**
 * An abstract deployment info implementation.
 * 
 * @author Jordan Halterman
 */
public abstract class AbstractDeploymentInfo implements DeploymentInfo {
  protected String id;
  protected String group;
  protected Map<String, Object> config;
  protected int instances = 1;

  @Override
  public String id() {
    return id;
  }

  @Override
  public String group() {
    return group;
  }

  @Override
  public JsonObject config() {
    return new JsonObject(config);
  }

  @Override
  public int instances() {
    return instances;
  }

  @Override
  public ModuleDeploymentInfo asModule() {
    return (ModuleDeploymentInfo) this;
  }

  @Override
  public VerticleDeploymentInfo asVerticle() {
    return (VerticleDeploymentInfo) this;
  }

}
