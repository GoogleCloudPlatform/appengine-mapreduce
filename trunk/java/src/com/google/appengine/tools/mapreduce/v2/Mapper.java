// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2;

/**
 * Mapper class. This is essentially an interface, but defined as abstract class to prevent breakages whenever possible.
 *
 * @param <K> input key
 * @param <V> input value
 * @param <OK> output key
 * @param <OV> output value
 *
 */
public abstract class Mapper<K, V, OK, OV> {
  public abstract void map(K key, V value, Output<OK, OV> output);

  public abstract class Output<K, V> {
    public abstract void emit(K key, V value);
  }
}
