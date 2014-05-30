// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;


/**
 * Context for each shard.
 */
public interface ShardContext extends Context {

  /**
   * Returns the total number of shards.
   */
  int getShardCount();

  /**
   * Returns the number of this mapper or reducer shard (zero-based).
   */
  int getShardNumber();

  /**
   * Returns a {@link Counters} object for doing simple aggregate calculations.
   */
  Counters getCounters();

  /**
   * Returns the {@link Counter} with the given name.
   */
  Counter getCounter(String name);

  /**
   * Increments the {@link Counter} with the given name by {@code delta}.
   */
  void incrementCounter(String name, long delta);

  /**
   * Increments the {@link Counter} with the given name by 1.
   */
  void incrementCounter(String name);
}
