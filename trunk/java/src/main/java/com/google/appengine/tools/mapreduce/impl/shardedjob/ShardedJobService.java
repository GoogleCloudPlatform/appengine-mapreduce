// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.io.Serializable;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * Allows interaction with sharded jobs.
 *
 * As part of its operation, the {@code ShardedJobService} will enqueue task
 * queue tasks that send requests to the URLs specified in
 * {@link ShardedJobSettings}.  It is the user's responsibility to arrange
 * for these requests to be passed back into {@link #handleControllerRequest}
 * and {@link #handleWorkerRequest}.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public interface ShardedJobService {

  /**
   * Starts a new sharded job with the given ID and parameters.  The ID must
   * be unique.
   *
   * This method is idempotent -- if another invocation of this method aborted
   * (or is in an unknown state, possibly still running or completed), starting
   * the job can be retried by calling the method again with the same arguments.
   * The job won't start twice unless {@link #cleanupJob} is called in between.
   *
   * @param <T> type of tasks that the job consists of
   * @param <R> type of intermediate and final results of the job
   */
  <T extends IncrementalTask<T, R>, R extends Serializable> void startJob(
      String jobId,
      List<? extends T> initialTasks,
      ShardedJobController<T, R> controller,
      ShardedJobSettings settings);

  /**
   * Returns the state of the job with the given ID.  Returns null if no such
   * job exists.
   */
  <R extends Serializable> ShardedJobState<?, R> getJobState(String jobId);

  /**
   * Aborts execution of the job with the given ID.  If the job has already
   * finished or does not exist, this is a no-op.
   */
  void abortJob(String jobId);

  /**
   * Deletes all data about the job with the given ID.  If the job is still
   * running, this will additionally abort it.  If the job does not exist, this
   * is a no-op.
   */
  void cleanupJob(String jobId);

  /**
   * Must be called from the servlet that handles
   * {@link ShardedJobSettings#setControllerPath}.
   */
  void handleControllerRequest(HttpServletRequest request);

  /**
   * Must be called from the servlet that handles
   * {@link ShardedJobSettings#setWorkerPath}.
   */
  void handleWorkerRequest(HttpServletRequest request);

}
