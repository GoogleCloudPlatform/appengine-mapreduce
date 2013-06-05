// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * Key-value pair.
 *
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KeyValue<K, V> implements Serializable {
  private static final long serialVersionUID = -2687854533615172943L;

  private final K key;
  private final V value;

  public KeyValue(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }

  @Override public String toString() {
    return "KeyValue(" + key + ", " + value + ")";
  }

  @Override public final boolean equals(Object o) {
    if (o == this) { return true; }
    if (!(o instanceof KeyValue)) { return false; }
    KeyValue<?, ?> other = (KeyValue<?, ?>) o;
    return Objects.equal(key, other.key)
        && Objects.equal(value, other.value);
  }

  @Override public final int hashCode() {
    return Objects.hashCode(key, value);
  }

  public static <K, V> KeyValue<K, V> of(K k, V v) {
    return new KeyValue<K, V>(k, v);
  }

}
