// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * Implementation of {@link ShardedJobService}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
class ShardedJobServiceImpl implements ShardedJobService {

  @Override public <T extends IncrementalTask<T, R>, R extends Serializable> void startJob(
      String jobId,
      List<? extends T> initialTasks,
      ShardedJobController<T, R> controller,
      ShardedJobSettings settings) {
    new ShardedJobRunner<T, R>().startJob(jobId, initialTasks, controller, settings);
  }

  @SuppressWarnings("unchecked")
  @Override public ShardedJobState<?, Serializable> getJobState(String jobId) {
    return new ShardedJobRunner().getJobState(jobId);
  }

  @Override public void abortJob(String jobId) {
    new ShardedJobRunner().abortJob(jobId);
  }

  @Override public void cleanupJob(String jobId) {
    throw new RuntimeException("Not implemented");
  }

  @Override public void handleControllerRequest(HttpServletRequest request) {
    new ShardedJobRunner().pollTaskStates(
        checkNotNull(request.getParameter(ShardedJobRunner.JOB_ID_PARAM), "Null job id"),
        Integer.parseInt(request.getParameter(ShardedJobRunner.SEQUENCE_NUMBER_PARAM)));
  }

  @Override public void handleWorkerRequest(HttpServletRequest request) {
    new ShardedJobRunner().runTask(
        checkNotNull(request.getParameter(ShardedJobRunner.TASK_ID_PARAM), "Null task id"),
        checkNotNull(request.getParameter(ShardedJobRunner.JOB_ID_PARAM), "Null job id"),
        Integer.parseInt(request.getParameter(ShardedJobRunner.SEQUENCE_NUMBER_PARAM)));
  }

}
