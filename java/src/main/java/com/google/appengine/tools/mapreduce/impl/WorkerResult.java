// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.OutputWriter;

import java.io.Serializable;
import java.util.Map;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <O> type of output values produced by the worker
 */
public class WorkerResult<O> implements Serializable {
  private static final long serialVersionUID = 102465178616294776L;

  // These are Maps rather than Lists since they are dense only when aggregated,
  // not for each individual shard (which accumulates its own WorkerResult).
  private final Map<Integer, OutputWriter<O>> closedWriters;
  private final Map<Integer, WorkerShardState> workerShardStates;
  private final CountersImpl counters;

  public WorkerResult(Map<Integer, OutputWriter<O>> closedWriters,
      Map<Integer, WorkerShardState> workerShardStates,
      CountersImpl counters) {
    this.closedWriters = checkNotNull(closedWriters, "Null closedWriters");
    this.workerShardStates = checkNotNull(workerShardStates, "Null workerShardStates");
    this.counters = checkNotNull(counters, "Null counters");
  }

  public Map<Integer, OutputWriter<O>> getClosedWriters() {
    return closedWriters;
  }

  public Map<Integer, WorkerShardState> getWorkerShardStates() {
    return workerShardStates;
  }

  public CountersImpl getCounters() {
    return counters;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + closedWriters + ", "
        + workerShardStates + ", "
        + counters
        + ")";
  }

}
