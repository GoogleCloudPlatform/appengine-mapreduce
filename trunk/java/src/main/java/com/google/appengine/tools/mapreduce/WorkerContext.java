// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Context for each worker (mapper or reducer) shard.
 *
 */
public abstract class WorkerContext {

  private final String jobId;
  private final int shardNumber;
  private final Counters counters;

  protected WorkerContext(String jobId, int shardNumber, Counters counters) {
    this.jobId = checkNotNull(jobId, "Null jobId");
    this.shardNumber = shardNumber;
    this.counters = checkNotNull(counters, "Null counters");
  }

  public String getJobId() {
    return jobId;
  }

  /**
   * Returns the number of this mapper or reducer shard (zero-based).
   */
  public int getShardNumber() {
    return shardNumber;
  }

  /**
   * Returns a {@link Counters} object for doing simple aggregate calculations.
   */
  public Counters getCounters() {
    return counters;
  }

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

}
