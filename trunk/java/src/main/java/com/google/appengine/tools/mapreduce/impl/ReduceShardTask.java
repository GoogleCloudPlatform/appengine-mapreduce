// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.CounterNames;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.ReducerContext;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardFailureException;

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

  public ReduceShardTask(String mrJobId,
      int shardNumber, int shardCount,
      InputReader<KeyValue<K, Iterator<V>>> in,
      Reducer<K, V, O> reducer,
      OutputWriter<O> out,
      long millisPerSlice) {
    super(mrJobId, shardNumber, shardCount,
        in, reducer, out,
        CounterNames.REDUCER_CALLS, CounterNames.REDUCER_WALLTIME_MILLIS);
    this.reducer = checkNotNull(reducer, "Null reducer");
    this.millisPerSlice = millisPerSlice;
  }

  @Override protected ReducerContext<O> getWorkerContext(Counters counters) {
    return new ReducerContextImpl<O>(mrJobId, shardNumber, out, counters);
  }

  @Override protected void callWorker(KeyValue<K, Iterator<V>> input) {
    ReducerInput<V> value = ReducerInputs.fromIterator(input.getValue());
    try {
      reducer.reduce(input.getKey(), value);
    } catch (RuntimeException ex) {
      throw new ShardFailureException(shardNumber, ex);
    }
  }

  @Override protected String formatLastWorkItem(KeyValue<K, Iterator<V>> item) {
    return item == null ? null : abbrev(item.getKey());
  }

  @Override
  protected boolean shouldCheckpoint(long timeElapsed) {
    return timeElapsed > millisPerSlice;
  }

  @Override
  protected boolean canContinue() {
    return true;
  }

  @Override
  protected long estimateMemoryNeeded() {
    return in.estimateMemoryRequirment() + out.estimateMemoryRequirment()
        + MapReduceConstants.ASSUMED_BASE_MEMORY_PER_REQUEST;
  }
}
