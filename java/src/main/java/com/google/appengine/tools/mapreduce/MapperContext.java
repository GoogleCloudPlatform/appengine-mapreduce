// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;


/**
 * A context for mapper execution. Provides everything that might be needed by a mapper function.
 *
 *
 * @param <K> type of keys produced by the mapper
 * @param <V> type of values produced by the mapper
 */
public interface MapperContext<K, V> extends WorkerContext<KeyValue<K, V>> {

  /**
   * Emits a key and a value to the output.
   */
  void emit(K key, V value);
}
