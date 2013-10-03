// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values consumed by this worker
 * @param <O> type of output values produced by this worker
 */
public abstract class AbstractWorkerController<
    I extends IncrementalTask<I, WorkerResult<O>>, O>
    implements ShardedJobController<I, WorkerResult<O>> {
  private static final long serialVersionUID = 887646042087205202L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(AbstractWorkerController.class.getName());

  private final String shardedJobName;

  public AbstractWorkerController(String shardedJobName) {
    this.shardedJobName = checkNotNull(shardedJobName, "Null shardedJobName");
  }

  @Override
  public String getName() {
    return shardedJobName;
  }

  private void checkDisjoint(Set<?> small, Set<?> large) {
    for (Object x : small) {
      Preconditions.checkState(!large.contains(x), "Not disjoint: %s, %s, %s", x, small, large);
    }
  }

  @Override public WorkerResult<O> combineResults(Iterable<WorkerResult<O>> partialResults) {
    Map<Integer, OutputWriter<O>> closedWriters = Maps.newHashMap();
    Map<Integer, WorkerShardState> workerShardStates = Maps.newHashMap();
    CountersImpl counters = new CountersImpl();
    for (WorkerResult<O> r : partialResults) {
      checkDisjoint(r.getClosedWriters().keySet(), closedWriters.keySet());
      closedWriters.putAll(r.getClosedWriters());
      counters.addAll(r.getCounters());
      for (Map.Entry<Integer, WorkerShardState> entry : r.getWorkerShardStates().entrySet()) {
        int i = entry.getKey();
        WorkerShardState a = workerShardStates.get(i);
        WorkerShardState b = entry.getValue();
        if (a == null) {
          workerShardStates.put(i, b);
        } else {
          workerShardStates.put(i,
              new WorkerShardState(
                  a.getWorkerCallCount() + b.getWorkerCallCount(),
                  Math.max(a.getMostRecentUpdateTimeMillis(),
                      b.getMostRecentUpdateTimeMillis()),
                  b.getLastWorkItem()));
        }
      }
    }
    return new WorkerResult<O>(closedWriters, workerShardStates, counters);
  }

  @Override public abstract void completed(WorkerResult<O> finalCombinedResult);

}
