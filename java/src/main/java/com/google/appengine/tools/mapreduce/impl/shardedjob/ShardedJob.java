// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Value;

import java.util.List;

/**
 * ShardedJob pipeline.
 *
 *
 * @param <T> type of task
 */
public class ShardedJob<T extends IncrementalTask> extends Job0<Void> {

  private static final long serialVersionUID = 5269173593221450859L;

  private final String shardedJobId;
  private final List<? extends T> workers;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings shardedJobSettings;

  public ShardedJob(String shardedJobId, List<? extends T> workers,
      ShardedJobController<T> controller, ShardedJobSettings shardedJobSettings) {
    this.shardedJobId = shardedJobId;
    this.workers = workers;
    this.controller = controller;
    this.shardedJobSettings = shardedJobSettings;
  }

  @Override
  public Value<Void> run() {
    ShardedJobServiceFactory.getShardedJobService().startJob(shardedJobId, workers, controller,
        shardedJobSettings);
    setStatusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl());
    return null;
  }
}
