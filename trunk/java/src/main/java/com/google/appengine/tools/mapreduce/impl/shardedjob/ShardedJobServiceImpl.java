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
  public ShardedJobState getJobState(String jobId) {
    return new ShardedJobRunner<>().getJobState(jobId);
  }

  @Override
  public Iterator<IncrementalTaskState<IncrementalTask>> lookupTasks(ShardedJobState state) {
    return new ShardedJobRunner<>().lookupTasks(state.getJobId(), state.getTotalTaskCount(), true);
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
