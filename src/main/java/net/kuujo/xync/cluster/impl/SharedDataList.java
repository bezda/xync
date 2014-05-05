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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 * Shared data-based list implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SharedDataList<T> implements List<T> {
  private final Map<Integer, Object> map;
  private int currentSize = 0;

  public SharedDataList(Map<Integer, Object> map) {
    this.map = map;
    this.currentSize = (int) (map.containsKey(-1) ? map.get(-1) : 0);
  }

  @Override
  public int size() {
    return currentSize;
  }

  @Override
  public boolean isEmpty() {
    return currentSize == 0;
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
    for (T value : c) {
      add(value);
    }
    return true;
  }

  @Override
  public boolean addAll(int index, Collection<? extends T> c) {
    int i = index;
    for (T value : c) {
      add(i, value);
      i++;
    }
    return true;
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
  public T get(int index) {
    if (index > currentSize-1) {
      throw new IndexOutOfBoundsException("Index out of bounds.");
    } else {
      return (T) map.get(index);
    }
  }

  @Override
  public boolean add(T e) {
    map.put(currentSize, e);
    currentSize++;
    map.put(-1, currentSize);
    return true;
  }

  @Override
  public void add(int index, T element) {
    map.put(currentSize, element);
    currentSize++;
    map.put(-1, currentSize);
  }

  @Override
  public int indexOf(Object o) {
    for (int i = 0; i < currentSize; i++) {
      if (map.get(i).equals(o)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public int lastIndexOf(Object o) {
    for (int i = currentSize-1; i > 0; i--) {
      if (map.get(i).equals(o)) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public ListIterator<T> listIterator() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public ListIterator<T> listIterator(int index) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean remove(Object o) {
    synchronized (map) {
      for (int i = 0; i < currentSize; i++) {
        T value = (T) map.get(i);
        if (value.equals(o)) {
          map.remove(i);
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void clear() {
    map.clear();
    currentSize = 0;
    map.put(-1, currentSize);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T set(int index, T element) {
    return (T) map.put(index, element);
  }

  @Override
  @SuppressWarnings("unchecked")
  public T remove(int index) {
    if (index > currentSize-1) {
      throw new IndexOutOfBoundsException("Index out of bounds.");
    } else {
      synchronized (map) {
        T value = (T) map.remove(index);
        int i = index+1;
        while (map.containsKey(i)) {
          map.put(i-1, map.remove(i));
          i++;
        }
        currentSize--;
        map.put(-1, currentSize);
        return value;
      }
    }
  }

}
