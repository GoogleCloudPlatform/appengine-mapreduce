package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.WorkerContext;

/**
 * Base class for all context implementations.
 *
 * @param <O> type of output values produced by the worker
 */
public abstract class AbstractWorkerContext<O> implements WorkerContext<O> {

  private final IncrementalTaskContext taskContext;

  protected AbstractWorkerContext(IncrementalTaskContext taskContext) {
    this.taskContext = taskContext;
  }

  @Override
  public String getJobId() {
    return taskContext.getJobId();
  }

  /**
   * Returns the number of this mapper or reducer shard (zero-based).
   */
  @Override
  public int getShardNumber() {
    return taskContext.getShardNumber();
  }

  /**
   * Returns the total number of shards.
   */
  @Override
  public int getShardCount() {
    return taskContext.getShardCount();
  }

  /**
   * Returns a {@link Counters} object for doing simple aggregate calculations.
   */
  @Override
  public final Counters getCounters() {
    return taskContext.getCounters();
  }

  /**
   * Returns the {@link Counter} with the given name.
   */
  @Override
  public final Counter getCounter(String name) {
    return getCounters().getCounter(name);
  }

  /**
   * Increments the {@link Counter} with the given name by {@code delta}.
   */
  @Override
  public final void incrementCounter(String name, long delta) {
    getCounter(name).increment(delta);
  }

  /**
   * Increments the {@link Counter} with the given name by 1.
   */
  @Override
  public final void incrementCounter(String name) {
    incrementCounter(name, 1);
  }
}
