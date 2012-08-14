// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

/**
 * Context for each worker (mapper or reducer) shard.
 *
 */
public abstract class WorkerContext {

  public abstract String getJobId();

  /**
   * Returns a {@link Counters} object for doing simple aggregate calculations.
   */
  public abstract Counters getCounters();

  /**
   * Returns the {@link Counter} with the given name.
   */
  public Counter getCounter(String name) {
    return getCounters().getCounter(name);
  }

  /**
   * Increments the {@link Counter} with the given name by {@code delta}.
   */
  public void incrementCounter(String name, long delta) {
    getCounter(name).increment(delta);
  }

  /**
   * Increments the {@link Counter} with the given name by 1.
   */
  public void incrementCounter(String name) {
    incrementCounter(name, 1);
  }

  /**
   * Returns the number of this mapper or reducer shard (zero-based).
   */
  public abstract int getShardNumber();

}
