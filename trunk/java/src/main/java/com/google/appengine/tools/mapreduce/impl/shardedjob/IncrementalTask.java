// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.io.Serializable;

/**
 * Portion of a sharded job that will be run in a single task queue task.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public interface IncrementalTask extends Serializable {

  /**
   * Runs this task. This will be invoked over and over until isDone returns true.
   *
   * If this throws an exception, it may be retried a limited number of times according to a retry
   * policy specified in ShardedJobSettings
   *
   * @throws RejectRequestException when this request should be ignored
   * @throws ShardFailureException when shard should be retried
   * @throws RuntimeException when a slice should be retried
   */
  void run();

  /**
   * @return true iff this task is done and run should no longer be invoked.
   */
  boolean isDone();
}
