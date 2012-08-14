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
   * Combine multiple result objects into a single result object.  The
   * {@code ShardedJob} execution framework assumes that this is associative
   * and commutative.
   *
   * Implementations may destroy the argument objects or return objects that
   * share state with arguments, but should not have other observable
   * side-effects.
   *
   * Note that {@link partialResults} may be empty.
   */
  /*Nullable*/ R combineResults(Iterable<R> partialResults);

  /**
   * Called when the sharded job has completed.
   */
  // TODO(ohler): Integrate with Pipeline more closely and eliminate this; the
  // result should be returned from a Pipeline Job.
  void completed(/*Nullable*/ R finalCombinedResult);

}
