package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.ShardContext;


/**
 * Base class for all ShardContext implementations.
 */
public abstract class BaseShardContext extends BaseContext implements ShardContext {

  private final int shardCount;
  private final int shardNumber;
  private final Counters counters;

  public BaseShardContext(IncrementalTaskContext taskContext) {
    super(taskContext.getJobId());
    this.counters = taskContext.getCounters();
    this.shardNumber = taskContext.getShardNumber();
    this.shardCount = taskContext.getShardCount();
  }

  @Override
  public int getShardCount() {
    return shardCount;
  }

  @Override
  public int getShardNumber() {
    return shardNumber;
  }

  @Override
  public Counters getCounters() {
    return counters;
  }

  @Override
  public Counter getCounter(String name) {
    return counters.getCounter(name);
  }

  @Override
  public final void incrementCounter(String name, long delta) {
    getCounter(name).increment(delta);
  }

  @Override
  public final void incrementCounter(String name) {
    incrementCounter(name, 1);
  }
}