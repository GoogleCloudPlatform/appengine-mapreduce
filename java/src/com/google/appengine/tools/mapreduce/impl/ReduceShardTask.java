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

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys consumed by this reducer
 * @param <V> type of intermediate values consumed by this reducer
 * @param <O> type of output values produced by this reducer
 */
public class ReduceShardTask<K, V, O>
    extends WorkerShardTask<KeyValue<K, ReducerInput<V>>, O, ReducerContext<O>> {
  private static final long serialVersionUID = 874429568286446321L;

  private final String mrJobId;
  private final int shardNumber;
  private final Reducer<K, V, O> reducer;
  private final OutputWriter<O> out;

  public ReduceShardTask(String mrJobId,
      int shardNumber, int shardCount,
      InputReader<KeyValue<K, ReducerInput<V>>> in,
      Reducer<K, V, O> reducer,
      OutputWriter<O> out,
      long millisPerSlice) {
    super(mrJobId, shardNumber, shardCount,
        in, reducer, out, millisPerSlice,
        CounterNames.REDUCER_CALLS, CounterNames.REDUCER_WALLTIME_MILLIS);
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.shardNumber = shardNumber;
    this.reducer = checkNotNull(reducer, "Null reducer");
    this.out = checkNotNull(out, "Null out");
  }

  @Override protected ReducerContext<O> getWorkerContext(Counters counters) {
    return new ReducerContextImpl<O>(mrJobId, shardNumber, out, counters);
  }

  @Override protected void callWorker(KeyValue<K, ReducerInput<V>> input) {
    reducer.reduce(input.getKey(), input.getValue());
  }

  @Override protected String formatLastWorkItem(KeyValue<K, ReducerInput<V>> item) {
    return item == null ? null : abbrev(item.getKey());
  }

}
