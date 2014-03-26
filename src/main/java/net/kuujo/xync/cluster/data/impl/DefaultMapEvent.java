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
package net.kuujo.xync.cluster.data.impl;

import net.kuujo.xync.cluster.data.MapEvent;

import org.vertx.java.core.json.JsonObject;

/**
 * A default event implementation.
 * 
 * @author Jordan Halterman
 */
public class DefaultMapEvent<K, V> implements MapEvent<K, V> {
  private Type type;
  private K key;
  private V value;

  public DefaultMapEvent(JsonObject info) {
    type = Type.parse(info.getString("event"));
    key = info.getValue("key");
    value = info.getValue("value");
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public V value() {
    return value;
  }

  @Override
  public JsonObject toJson() {
    return new JsonObject()
        .putString("type", type.toString())
        .putValue("key", key)
        .putValue("value", value);
  }

}
