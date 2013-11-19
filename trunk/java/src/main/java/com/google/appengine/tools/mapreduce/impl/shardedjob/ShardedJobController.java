// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.io.Serializable;

/**
 * Aggregates results from {@link IncrementalTask}s and receives notification
 * when the job completes.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of tasks that this job consists of
 * @param <R> type of intermediate and final results
 */
public interface ShardedJobController<T extends IncrementalTask<T, R>,
    R extends Serializable> extends Serializable {

  /**
   * @return A human readable string for UI purposes.
   */
  public String getName();

  /**
   * Combine multiple result objects into a single result object.  The
   * {@code ShardedJob} execution framework assumes that this is associative
   * and commutative.
   *
   * Implementations may destroy the argument objects or return objects that
   * share state with arguments, but should not have other observable
   * side-effects.
   *
   * Note that {@code partialResults} may be empty.
   */
  // TODO(user): combineResults is used in very strange ways and have various side-effects
  // and confusing code. Consider the following (backward incompatible to outstanding MRs) changes:
  // 1. Change IncrementalTask#run to accept previous run result(could be null) and return
  //      via its #getPartialResult the combined result (shard up-to-now).
  // 2. Move combine logic to WorkerResult (called by WorkerShardTask#doWork,
  //      AbstractWorkerController#completed and StatusHandler.handleGetJobDetails).
  // 3. Change #completed() to receive a List of WorkerResult (one per shard).
  // 4. Remove ShardedJobState#getAggregateResult and AGGREGATE_RESULT_PROPERTY datastore property
  // 5. Add to ShardedJobState getCompletedResults and StatusHandler.handleGetJobDetails will
  //      call it and use WorkerResult to combine it.
  /*Nullable*/ R combineResults(Iterable<R> partialResults);

  /**
   * Called when the sharded job has completed successfully.
   */
  // TODO(ohler): Integrate with Pipeline more closely and eliminate this; the
  // result should be returned from a Pipeline Job.
  void completed(/*Nullable*/ R finalCombinedResult);

  /**
   * Called when the sharded job has failed to complete successfully.
   * @param status
   */
  void failed(Status status);
}
