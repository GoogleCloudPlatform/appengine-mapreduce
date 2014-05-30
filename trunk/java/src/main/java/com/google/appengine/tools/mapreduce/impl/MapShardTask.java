// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.MAPPER_WALLTIME_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;

import java.io.IOException;
import java.io.ObjectInputStream;

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

  private transient MapperContextImpl<K, V> context;

  public MapShardTask(String mrJobId, int shardNumber, int shardCount, InputReader<I> in,
      Mapper<I, K, V> mapper, OutputWriter<KeyValue<K, V>> out, long millisPerSlice) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, MAPPER_CALLS,
        MAPPER_WALLTIME_MILLIS));
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.mapper = checkNotNull(mapper, "Null mapper");
    this.millisPerSlice = millisPerSlice;
    fillContext();
  }

  @Override
  protected void callWorker(I input) {
    mapper.map(input);
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
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + out.estimateMemoryRequirement()
        + mapper.estimateMemoryRequirement();
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
  public boolean allowSliceRetry(boolean abandon) {
    boolean skipWriterCheck = !abandon && !context.emitCalled();
    return (skipWriterCheck || out.allowSliceRetry()) && mapper.allowSliceRetry();
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    fillContext();
  }

  private void fillContext() {
    context = new MapperContextImpl<>(getContext(), out);
    in.setContext(context);
    out.setContext(context);
    mapper.setContext(context);
  }
}
