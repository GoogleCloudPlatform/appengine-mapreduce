// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.reducers;

import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.common.base.Preconditions;

/**
 * Reducer that emits the keys that occur in its input, discarding the values.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of keys
 * @param <V> type of values (discarded)
 */
public class KeyProjectionReducer<K, V> extends Reducer<K, V, K> {
  private static final long serialVersionUID = 466599637876532403L;

  public static <K, V> KeyProjectionReducer<K, V> create() {
    return new KeyProjectionReducer<K, V>();
  }

  private KeyProjectionReducer() {
  }

  @Override public void reduce(K key, ReducerInput<V> values) {
    Preconditions.checkState(values.hasNext(), "%s: No values: %s", this, key);
    getContext().emit(key);
  }

}
