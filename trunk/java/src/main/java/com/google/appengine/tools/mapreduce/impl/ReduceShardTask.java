// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.CounterNames.REDUCER_CALLS;
import static com.google.appengine.tools.mapreduce.CounterNames.REDUCER_WALLTIME_MILLIS;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerContext;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.Worker;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Iterator;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys consumed by this reducer
 * @param <V> type of intermediate values consumed by this reducer
 * @param <O> type of output values produced by this reducer
 */
public class ReduceShardTask<K, V, O>
    extends WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>> {

  private static final long serialVersionUID = 874429568286446321L;

  private final Reducer<K, V, O> reducer;
  private final long millisPerSlice;
  private final InputReader<KeyValue<K, Iterator<V>>> in;
  private final OutputWriter<O> out;

  private transient ReducerContextImpl<O> context;

  public ReduceShardTask(String mrJobId, int shardNumber, int shardCount,
      InputReader<KeyValue<K, Iterator<V>>> in, Reducer<K, V, O> reducer, OutputWriter<O> out,
      long millisPerSlice) {
    super(new IncrementalTaskContext(mrJobId, shardNumber, shardCount, REDUCER_CALLS,
        REDUCER_WALLTIME_MILLIS));
    this.in = checkNotNull(in, "Null in");
    this.out = checkNotNull(out, "Null out");
    this.reducer = checkNotNull(reducer, "Null reducer");
    this.millisPerSlice = millisPerSlice;
    fillContext();
  }

  @Override
  protected void callWorker(KeyValue<K, Iterator<V>> input) {
    ReducerInput<V> value = ReducerInputs.fromIterator(input.getValue());
    reducer.reduce(input.getKey(), value);
  }

  @Override
  protected String formatLastWorkItem(KeyValue<K, Iterator<V>> item) {
    return item == null ? null : abbrev(item.getKey());
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed >= millisPerSlice;
  }

  @Override
  protected long estimateMemoryRequirement() {
    return in.estimateMemoryRequirement() + getOutputWriter().estimateMemoryRequirement()
        + reducer.estimateMemoryRequirement();
  }

  @Override
  protected Worker<ReducerContext<O>> getWorker() {
    return reducer;
  }

  @Override
  public OutputWriter<O> getOutputWriter() {
    return out;
  }

  @Override
  public InputReader<KeyValue<K, Iterator<V>>> getInputReader() {
    return in;
  }

  @Override
  public boolean allowSliceRetry() {
    return (!context.emitCalled() || out.allowSliceRetry()) && reducer.allowSliceRetry();
  }

  private void readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    fillContext();
  }

  private void fillContext() {
    context = new ReducerContextImpl<>(getContext(), out);
    in.setContext(context);
    out.setContext(context);
    reducer.setContext(context);
  }
}
