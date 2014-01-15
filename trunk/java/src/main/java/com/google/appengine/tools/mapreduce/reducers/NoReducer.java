// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.reducers;

import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;

/**
 * A reducer that throws an exception if it receives any keys or values, and
 * never emits any values.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of keys formally (but not actually) accepted by this reducer
 * @param <V> type of values formally (but not actually) accepted by this reducer
 * @param <O> type of output formally (but not actually) emitted by this reducer
 */
public class NoReducer<K, V, O> extends Reducer<K, V, O> {

  private static final long serialVersionUID = 904068928342205092L;

  public static <K, V, O> NoReducer<K, V, O> create() {
    return new NoReducer<>();
  }

  private NoReducer() {
  }

  @Override
  public void reduce(K key, ReducerInput<V> values) {
    // TODO(user): throw JobFailureException once b/11898267 is implemented.
    throw new RuntimeException(
        getClass().getSimpleName() + ": reduce function was called for " + key);
  }
}
