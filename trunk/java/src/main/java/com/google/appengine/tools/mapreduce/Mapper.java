// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Map function for MapReduce computations.  A map function processes input
 * values one at a time and generates zero or more output key-value pairs for
 * each.  It emits the generated pairs to the {@link Reducer} through the
 * {@link MapperContext}.
 *
 * <p>This class is really an interface that might be evolving. In order to avoid breaking
 * users when we change the interface, we made it an abstract class.</p>
 *
 *
 * @param <I> type of input received
 * @param <K> type of intermediate keys produced
 * @param <V> type of intermediate values produced
 */
public abstract class Mapper<I, K, V> extends BaseMapper<I, KeyValue<K, V>, MapperContext<K, V>> {

  private static final long serialVersionUID = 1966174340710715049L;

  /**
   * Syntactic sugar for {@code emit(KeyValue.of(key, value))}
   */
  protected void emit(K key, V value) {
    getContext().emit(key, value);
  }
}
