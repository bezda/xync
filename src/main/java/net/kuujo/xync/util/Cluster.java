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
package net.kuujo.xync.util;

import java.util.HashSet;
import java.util.Set;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

/**
 * Cluster utilities.
 *
 * @author Jordan Halterman
 */
public final class Cluster {

  /**
   * Returns a boolean indicating whether a Hazelcast cluster is available.
   *
   * @return Indicates whether a Hazelcast cluster is available.
   */
  public static boolean isHazelcastCluster() {
    return getHazelcastInstance() != null;
  }

  /**
   * Returns the Vert.x Hazelcast instance.
   *
   * @return The Vert.x Hazelcast instance.
   */
  public static HazelcastInstance getHazelcastInstance() {
    for (HazelcastInstance instance : Hazelcast.getAllHazelcastInstances()) {
      MapConfig map = instance.getConfig().findMapConfig("subs");
      if (map != null && map.getName().equals("subs")) {
        return instance;
      }
    }
    return null;
  }

  /**
   * Returns the Vert.x Hazelcast instance local node ID.
   *
   * @return The local node ID for the Vert.x Hazelcast instance.
   */
  public static String getHazelcastNodeId() {
    HazelcastInstance instance = getHazelcastInstance();
    if (instance != null) {
      return instance.getCluster().getLocalMember().getUuid();
    }
    return null;
  }

  /**
   * Returns a set of members in the Vert.x Hazelcast cluster.
   *
   * @return A set of members in the Vert.x Hazelcast cluster.
   */
  public static Set<String> getHazelcastMembers() {
    HazelcastInstance instance = getHazelcastInstance();
    if (instance != null) {
      Set<String> members = new HashSet<>();
      for (Member member : instance.getCluster().getMembers()) {
        members.add(member.getUuid());
      }
      return members;
    }
    return null;
  }

  /**
   * Initializes a Vert.x Hazelcast instance for testing.
   */
  public static void initialize() {
    if (!isHazelcastCluster()) {
      Hazelcast.newHazelcastInstance(new Config().addMapConfig(new MapConfig().setName("subs")));
    }
  }

}
