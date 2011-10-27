// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2;

/**
 * Mapper class. This is essentially an interface, but defined as abstract class to prevent breakages whenever possible.
 *
 * @param <K> reducer's input key (mapper's output key)
 * @param <V> reducer's input value (mapper's output value)
 * @param <F> reducer's final output value.
 *
 */
public abstract class Reducer<K, V, F> {
  public abstract void reduce(K key, Input<V> input, Output<F> output);

  public abstract class Output<F> {
    public abstract void emit(F value);
  }

  public abstract class Input<V> {
    public abstract Iterable<V> values();
  }
}
