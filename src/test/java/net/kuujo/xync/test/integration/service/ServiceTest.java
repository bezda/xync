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
package net.kuujo.xync.test.integration.service;

import static org.vertx.testtools.VertxAssert.testComplete;

import net.kuujo.xync.test.integration.XyncTestVerticle;

import org.junit.Test;

/**
 * A network auditor test.
 *
 * @author Jordan Halterman
 */
public class ServiceTest extends XyncTestVerticle {

  @Test
  public void testDeployModule() {
    testComplete();
  }

  @Test
  public void testDeployModuleToGroup() {
    testComplete();
  }

  @Test
  public void testUndeployModule() {
    testComplete();
  }

  @Test
  public void testUndeployModuleToGroup() {
    testComplete();
  }

  @Test
  public void testMissingGet() {
    testComplete();
  }

  @Test
  public void testSetGet() {
    testComplete();
  }

  @Test
  public void testDelete() {
    testComplete();
  }

  @Test
  public void testExists() {
    testComplete();
  }

  @Test
  public void testWatchCreate() {
    testComplete();
  }

  @Test
  public void testWatchUpdate() {
    testComplete();
  }

  @Test
  public void testWatchDelete() {
    testComplete();
  }

}
