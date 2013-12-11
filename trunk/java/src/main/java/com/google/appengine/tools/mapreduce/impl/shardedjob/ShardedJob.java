// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;

import java.io.Serializable;
import java.util.List;

/**
 * ShardedJob pipeline.
 *
 *
 * @param <T> type of task
 * @param <R> type of intermediate and final results of the job
 */
public class ShardedJob<T extends IncrementalTask<T, R>, R extends Serializable>
    extends Job0<Void> {

  private static final long serialVersionUID = 5269173593221450859L;

  private final String shardedJobId;
  private final List<? extends T> workers;
  private final ShardedJobController<T, R> controller;
  private final ShardedJobSettings shardedJobSettings;

  public ShardedJob(String shardedJobId, List<? extends T> workers,
      ShardedJobController<T, R> controller, ShardedJobSettings shardedJobSettings) {
    this.shardedJobId = shardedJobId;
    this.workers = workers;
    this.controller = controller;
    this.shardedJobSettings = shardedJobSettings;
  }

  @Override
  public Value<Void> run() {
    ShardedJobServiceFactory.getShardedJobService().startJob(
        shardedJobId, workers, controller, shardedJobSettings);
    return null;
  }
}
