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

/**
 * Verticle deployment info.
 *
 * @author Jordan Halterman
 */
public interface VerticleDeploymentInfo {

  /**
   * Returns the verticle main.
   *
   * @return The verticle main.
   */
  String main();

  /**
   * Indicates whether the verticle is a worker.
   *
   * @return Indicates whether the verticle is a worker.
   */
  boolean isWorker();

  /**
   * Returns the verticle deployment info as worker deployment info.
   *
   * @return Worker deployment info.
   */
  WorkerVerticleDeploymentInfo asWorker();

}
