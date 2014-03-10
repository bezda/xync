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

import net.kuujo.zync.cluster.Event;

import org.vertx.java.core.json.JsonObject;

/**
 * A default event implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultEvent implements Event {
  private Type type;
  private String key;
  private Object value;

  public DefaultEvent(JsonObject info) {
    type = Type.parse(info.getString("event"));
    key = info.getString("key");
    value = info.getValue("value");
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T value() {
    return (T) value;
  }

  @Override
  public JsonObject toJson() {
    return new JsonObject()
        .putString("type", type.toString())
        .putString("key", key)
        .putValue("value", value);
  }

}
