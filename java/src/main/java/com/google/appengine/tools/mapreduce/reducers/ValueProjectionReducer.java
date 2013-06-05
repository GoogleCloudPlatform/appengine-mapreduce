// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.reducers;

import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerInput;

/**
 * Reducer that emits the values that occur in its input, discarding the keys.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of keys (discarded)
 * @param <V> type of values
 */
public class ValueProjectionReducer<K, V> extends Reducer<K, V, V> {
  private static final long serialVersionUID = 990027274731447358L;

  public static <K, V> ValueProjectionReducer<K, V> create() {
    return new ValueProjectionReducer<K, V>();
  }

  private ValueProjectionReducer() {
  }

  @Override public void reduce(K key, ReducerInput<V> values) {
    while (values.hasNext()) {
      getContext().emit(values.next());
    }
  }

}
