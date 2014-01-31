// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link ShardedJobService}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class ShardedJobServiceImpl implements ShardedJobService {

  @Override
  public <T extends IncrementalTask> void startJob(
      String jobId,
      List<? extends T> initialTasks,
      ShardedJobController<T> controller,
      ShardedJobSettings settings) {
    new ShardedJobRunner<T>().startJob(jobId, initialTasks, controller, settings);
  }

  @Override
  public <T extends IncrementalTask> ShardedJobState<T> getJobState(String jobId) {
    return new ShardedJobRunner<T>().getJobState(jobId);
  }

  @Override
  public <T extends IncrementalTask> Iterator<IncrementalTaskState<T>> lookupTasks(
      ShardedJobState<T> state) {
    return new ShardedJobRunner<T>().lookupTasks(state.getJobId(), state.getTotalTaskCount());
  }

  @Override
  public void abortJob(String jobId) {
    new ShardedJobRunner<>().abortJob(jobId);
  }

  @Override
  public boolean cleanupJob(String jobId) {
    return new ShardedJobRunner<>().cleanupJob(jobId);
  }
}
