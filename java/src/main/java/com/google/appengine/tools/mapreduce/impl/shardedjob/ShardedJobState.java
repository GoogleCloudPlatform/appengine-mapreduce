// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.io.Serializable;

/**
 * Information about execution and progress of a sharded job.
 *
 * Undefined behavior results if any of the values (such as the return value of
 * getSettings()) are mutated.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of tasks that the job consists of
 * @param <R> type of intermediate and final results of the job
 */
public interface ShardedJobState<T extends IncrementalTask<T, R>, R extends Serializable> {

  /**
   * Returns the ID of this job.
   */
  String getJobId();

  /**
   * Returns the controller for this job.
   */
  ShardedJobController<T, R> getController();

  /**
   * Returns the execution settings of this job.
   */
  ShardedJobSettings getSettings();

  /**
   * Returns the total number of tasks (not including follow-up tasks) that this
   * job consists of.
   */
  int getTotalTaskCount();

  /**
   * Returns the number of tasks or follow-up tasks that are currently active.
   */
  int getActiveTaskCount();

  /**
   * Returns the time this job was started.
   */
  long getStartTimeMillis();

  /**
   * Returns the time this job's state was last updated.
   */
  long getMostRecentUpdateTimeMillis();

  /**
   * Returns whether this job is running, finished, etc.
   */
  Status getStatus();

  /**
   * Returns the aggregate result as computed by
   * {@link ShardedJobController#combineResults}.
   *
   * As long as the job is still active ({@link #getStatus} returns
   * {@link Status.StatusCode#INITIALIZING} or {@link Status.StatusCode#RUNNING}), the aggregate
   * result only reflects the progress so far; when {@link #getStatus} returns
   * one of the final states, the aggregate result is the final result.
   */
  /*Nullable*/ R getAggregateResult();
}
