// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

/**
 * Counter is an integer variable that is aggregated across multiple shards. Can be used to do
 * statistical calculations.
 *
 */
public interface Counter {

  /**
   * @return counter name.
   */
  String getName();

  /**
   * @return counter value. This is the value only in the current shard. It doesn't include
   * contributions from other shards, if accessed from within mapper/reducer.
   */
  long getValue();

  /**
   * Increment counter.
   *
   * @param delta increment delta. Can be negative.
   */
  void increment(long delta);
}
