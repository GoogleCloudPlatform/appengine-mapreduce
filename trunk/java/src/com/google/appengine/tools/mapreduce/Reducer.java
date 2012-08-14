// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Reduce function for use in MapReduce.  Called once for each key, together
 * with the sequence of all values for that key that the map phase produced.
 * Can emit output values through the context.
 *
 * <p>This class is really an interface that might be evolving.  In order to
 * avoid breaking users when we change the interface, we made it an abstract
 * class.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys received
 * @param <V> type of intermediate values received
 * @param <O> type of output values produced
 */
public abstract class Reducer<K, V, O> extends Worker<ReducerContext<O>> {
  private static final long serialVersionUID = 1622389951004432376L;

  /**
   * Processes the values for a given key, using the context returned by
   * {@link Worker#getContext} to emit output to the {@link Output} of the MapReduce.
   *
   * {@code values} enumerates all values that the map phase produced for the
   * key {@code key}.  It will always contain at least one value.
   */
  public abstract void reduce(K key, ReducerInput<V> values);

}
