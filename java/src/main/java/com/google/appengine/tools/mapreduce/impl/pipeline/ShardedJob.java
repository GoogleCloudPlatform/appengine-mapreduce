// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
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

  private static final long serialVersionUID = -6595147973116356334L;

  private final String jobId;
  private final List<? extends T> workers;
  private final ShardedJobController<T> controller;
  private final ShardedJobSettings settings;

  public ShardedJob(String shardedJobId, List<? extends T> workers,
      ShardedJobController<T> controller, ShardedJobSettings shardedJobSettings) {
    this.jobId = shardedJobId;
    this.workers = workers;
    this.controller = controller;
    this.settings = shardedJobSettings;
  }

  @Override
  public Value<Void> run() {
    ShardedJobServiceFactory.getShardedJobService().startJob(jobId, workers, controller, settings);
    setStatusConsoleUrl(settings.getMapReduceStatusUrl());
    return null;
  }
}
