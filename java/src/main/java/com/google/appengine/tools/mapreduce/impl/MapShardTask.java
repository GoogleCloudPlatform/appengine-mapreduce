// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_WALLTIME_MILLIS;
import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.ASSUMED_BASE_MEMORY_PER_REQUEST;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardFailureException;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values consumed by this mapper
 * @param <K> type of intermediate keys produced by this mapper
 * @param <V> type of intermediate values produced by this mapper
 */
public class MapShardTask<I, K, V> extends WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> {

  private static final long serialVersionUID = 978040803132974582L;

  private final Mapper<I, K, V> mapper;
  private final long millisPerSlice;
  private final InputReader<I> in;
  private final OutputWriter<KeyValue<K, V>> out;
  private final IncrementalTaskContext context;

  public MapShardTask(String mrJobId, int shardNumber, int shardCount, InputReader<I> in,
      Mapper<I, K, V> mapper, OutputWriter<KeyValue<K, V>> out, long millisPerSlice) {
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.mapper = checkNotNull(mapper, "Null mapper");
    this.millisPerSlice = millisPerSlice;
    this.context = new IncrementalTaskContext(mrJobId, shardNumber, shardCount, MAPPER_CALLS,
        MAPPER_WALLTIME_MILLIS);
  }

  @Override
  protected void callWorker(I input) {
    try {
      mapper.map(input);
    } catch (RuntimeException ex) {
      throw new ShardFailureException(context.getShardNumber(), ex);
    }
  }

  @Override
  protected String formatLastWorkItem(I item) {
    return abbrev(item);
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed >= millisPerSlice;
  }

  @Override
  protected long estimateMemoryNeeded() {
    return in.estimateMemoryRequirement() + getOutputWriter().estimateMemoryRequirement()
        + ASSUMED_BASE_MEMORY_PER_REQUEST;
  }

  @Override
  protected Worker<MapperContext<K, V>> getWorker() {
    return mapper;
  }

  @Override
  public OutputWriter<KeyValue<K, V>> getOutputWriter() {
    return out;
  }

  @Override
  public InputReader<I> getInputReader() {
    return in;
  }

  @Override
  protected void setContextOnWorker() {
    mapper.setContext(new MapperContextImpl<>(out, context));
  }

  @Override
  public IncrementalTaskContext getContext() {
    return context;
  }
}
