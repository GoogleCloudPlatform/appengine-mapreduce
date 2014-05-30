// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;


/**
 * A context for mapper execution. Provides everything that might be needed by a mapper function.
 *
 *
 * @param <K> type of intermediate keys produced by the mapper
 * @param <V> type of intermediate values produced by the mapper
 */
public interface MapperContext<K, V> extends WorkerContext<KeyValue<K, V>> {

  /**
   * Emits a value for the given key to the reduce stage.
   */
  void emit(K key, V value);
}
