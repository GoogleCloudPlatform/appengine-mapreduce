package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.WorkerContext;

import java.io.IOException;


/**
 * Base class for all ShardContext implementations.
 *
 * @param <O> type of emitted values
 */
public abstract class BaseShardContext<O> extends BaseContext implements WorkerContext<O> {

  private final int shardCount;
  private final int shardNumber;
  private final Counters counters;
  private final OutputWriter<O> outputWriter;
  private boolean emitCalled;

  public BaseShardContext(IncrementalTaskContext taskContext, OutputWriter<O> outputWriter) {
    super(taskContext.getJobId());
    this.counters = taskContext.getCounters();
    this.shardNumber = taskContext.getShardNumber();
    this.shardCount = taskContext.getShardCount();
    this.outputWriter = checkNotNull(outputWriter, "Null output");
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

  @Override
  public void emit(O value) {
    emitCalled = true;
    try {
      outputWriter.write(value);
    } catch (IOException e) {
      throw new RuntimeException(outputWriter + ".write(" + value + ") threw IOException", e);
    }
  }

  boolean emitCalled() {
    return emitCalled;
  }
}