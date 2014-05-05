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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

/**
 * A shared data based queue implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <T> The queue data type.
 */
public class SharedDataQueue<T> implements Queue<T> {
  private final Map<Integer, Object> map;
  private int currentIndex;

  public SharedDataQueue(Map<Integer, Object> map) {
    this.map = map;
    this.currentIndex = (int) (map.containsKey(-1) ? map.get(-1) : 0);
  }

  @Override
  public int size() {
    return map.size() - 1;
  }

  @Override
  public boolean isEmpty() {
    return map.size() == 1;
  }

  @Override
  public boolean contains(Object o) {
    return map.values().contains(o);
  }

  @Override
  public Iterator<T> iterator() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return map.values().containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends T> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public T remove() {
    synchronized (map) {
      if (map.containsKey(currentIndex)) {
        T value = (T) map.remove(currentIndex);
        currentIndex++;
        map.put(-1, currentIndex);
        return value;
      } else {
        throw new IllegalStateException("Queue is empty.");
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T poll() {
    T value = (T) map.remove(currentIndex);
    if (value != null) {
      currentIndex++;
      map.put(-1, currentIndex);
    }
    return value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T element() {
    T value = (T) map.get(currentIndex);
    if (value != null) {
      return value;
    } else {
      throw new IllegalStateException("Queue is empty.");
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T peek() {
    return (T) map.get(currentIndex);
  }

  @Override
  public boolean offer(T e) {
    int index = currentIndex + map.size() - 1;
    map.put(index, e);
    return true;
  }

  @Override
  public boolean add(T e) {
    int index = currentIndex + map.size() - 1;
    map.put(index, e);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    synchronized (map) {
      Iterator<Map.Entry<Integer, Object>> iter = map.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Integer, Object> entry = iter.next();
        if (entry.getValue().equals(o)) {
          iter.remove();
          int index = entry.getKey()+1;
          while (map.containsKey(index)) {
            map.put(index-1, map.remove(index));
            index++;
          }
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void clear() {
    map.clear();
    map.put(-1, currentIndex);
  }

}
