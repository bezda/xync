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

import net.kuujo.xync.cluster.impl.DefaultEvent;

import org.vertx.java.core.json.JsonObject;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A cluster event.
 * 
 * @author Jordan Halterman
 */
@JsonTypeInfo(
  use=JsonTypeInfo.Id.CLASS,
  include=JsonTypeInfo.As.PROPERTY,
  property="class",
  defaultImpl=DefaultEvent.class
)
public interface Event {

  /**
   * An event type.
   * 
   * @author Jordan Halterman
   */
  public static enum Type {

    /**
     * A create event.
     */
    CREATE("create"),

    /**
     * A change event.
     */
    CHANGE("change"),

    /**
     * An update event.
     */
    UPDATE("update"),

    /**
     * A delete event.
     */
    DELETE("delete");

    private final String name;

    private Type(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

    /**
     * Parses an event type name.
     * 
     * @param name The event type name.
     * @return An event type.
     * @throws IllegalArgumentException If the event type name is invalid.
     */
    public static Type parse(String name) {
      switch (name) {
        case "create":
          return CREATE;
        case "update":
          return UPDATE;
        case "delete":
          return DELETE;
        default:
          throw new IllegalArgumentException("Invalid event type " + name);
      }
    }

  }

  /**
   * Returns the event type.
   * 
   * @return The event type.
   */
  Type type();

  /**
   * Returns the key on which the event occurred.
   * 
   * @return The key on which the event occurred.
   */
  String key();

  /**
   * Returns the event key value.
   * 
   * @return The event key value.
   */
  <T> T value();

  /**
   * Returns a json representation of the event.
   *
   * @return The json event.
   */
  JsonObject toJson();

}