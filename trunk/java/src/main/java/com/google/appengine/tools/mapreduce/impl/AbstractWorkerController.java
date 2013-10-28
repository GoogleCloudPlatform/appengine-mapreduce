// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values consumed by this worker
 * @param <O> type of output values produced by this worker
 */
public abstract class
    AbstractWorkerController<I extends IncrementalTask<I, WorkerResult<O>>, O>
    implements ShardedJobController<I, WorkerResult<O>> {

  private static final long serialVersionUID = 887646042087205202L;

  private final String shardedJobName;

  public AbstractWorkerController(String shardedJobName) {
    this.shardedJobName = checkNotNull(shardedJobName, "Null shardedJobName");
  }

  @Override
  public String getName() {
    return shardedJobName;
  }

  private static <K, V> void checkDisjointAndAdd(Map<K, V> values, Map<K, V> addTo) {
    for (Entry<K, V> entry: values.entrySet()) {
      Preconditions.checkState(!addTo.containsKey(entry.getKey()),
          "Not disjoint: %s, %s, %s", entry.getKey(), values, addTo);
      addTo.put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public WorkerResult<O> combineResults(Iterable<WorkerResult<O>> partialResults) {
    Map<Integer, OutputWriter<O>> closedWriters = new HashMap<>();
    Map<Integer, WorkerShardState> workerShardStates = new HashMap<>();
    CountersImpl counters = new CountersImpl();
    for (WorkerResult<O> r : partialResults) {
      checkDisjointAndAdd(r.getClosedWriters(), closedWriters);
      counters.addAll(r.getCounters());
      for (Map.Entry<Integer, WorkerShardState> entry : r.getWorkerShardStates().entrySet()) {
        int shard = entry.getKey();
        WorkerShardState combinedState = workerShardStates.get(shard);
        WorkerShardState state = entry.getValue();
        if (combinedState == null) {
          workerShardStates.put(shard, state);
        } else {
          workerShardStates.put(shard, state.combine(combinedState));
        }
      }
    }
    return new WorkerResult<O>(closedWriters, workerShardStates, counters);
  }
}
