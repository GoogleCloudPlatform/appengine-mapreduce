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
   * Called immediately before run.
   *
   * This method should be very limited in scope and should not block, perform IO or fail for any
   * reason other than Rejecting the request.
   *
   * @throws RejectRequestException when run cannot be called at this time.
   */
  void prepare();

  /**
   * Runs this task. This will be invoked over and over until isDone returns true.
   *
   * If this throws an exception, it may be retried a limited number of times according to a retry
   * policy specified in ShardedJobSettings
   *

   * @throws ShardFailureException when shard should be retried
   * @throws RuntimeException when a slice should be retried
   */
  void run();

  /**
   * Frees any resources claimed in prepare.
   * Implementations of this method should not throw under any circumstances.
   */
  void release();

  /**
   * @return true iff this task is done and run should no longer be invoked.
   */
  boolean isDone();
}
