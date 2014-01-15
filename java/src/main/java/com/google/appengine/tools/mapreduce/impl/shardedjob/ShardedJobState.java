// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;



/**
 * Information about execution and progress of a sharded job.
 *
 * Undefined behavior results if any of the values (such as the return value of
 * getSettings()) are mutated.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of the IncrementalTask
 */
public interface ShardedJobState<T extends IncrementalTask> {

  /**
   * Returns the ID of this job.
   */
  String getJobId();

  /**
   * Returns the controller for this job.
   */
  ShardedJobController<T> getController();

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
}
