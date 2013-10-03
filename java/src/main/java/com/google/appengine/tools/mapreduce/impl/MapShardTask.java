// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.CounterNames;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.OutputWriter;

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

  public MapShardTask(String mrJobId,
      int shardNumber, int shardCount,
      InputReader<I> in,
      Mapper<I, K, V> mapper,
      OutputWriter<KeyValue<K, V>> out,
      long millisPerSlice) {
    super(mrJobId, shardNumber, shardCount,
        in, mapper, out, 
        CounterNames.MAPPER_CALLS, CounterNames.MAPPER_WALLTIME_MILLIS);
    this.mapper = checkNotNull(mapper, "Null mapper");
    this.millisPerSlice = millisPerSlice;
  }

  @Override 
  protected MapperContext<K, V> getWorkerContext(Counters counters) {
    return new MapperContextImpl<K, V>(mrJobId, out, shardNumber, counters);
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
    return timeElapsed > millisPerSlice;
  }

  @Override
  protected boolean canContinue() {
    return true;
  }

}
