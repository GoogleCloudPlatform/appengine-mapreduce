// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import java.io.Serializable;

/**
 * Portion of a sharded job that will be run in a single task queue task.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of task
 * @param <R> type of intermediate and final results of the job
 */
public interface IncrementalTask<T extends IncrementalTask<T, R>, R extends Serializable>
    extends Serializable {

  /**
   * A result from a completed {@link IncrementalTask}, and optionally, a
   * follow-up task.
   */
  static class RunResult<T extends IncrementalTask<T, R>, R extends Serializable> {
    public static <T extends IncrementalTask<T, R>, R extends Serializable> RunResult<T, R> of(
        R partialResult, T followupTask) {
      return new RunResult<T, R>(partialResult, followupTask);
    }

    /*Nullable*/ private final R partialResult;
    // Null means no followup task.
    /*Nullable*/ private final T followupTask;

    public RunResult(/*Nullable*/ R partialResult,
        /*Nullable*/ T followupTask) {
      this.partialResult = partialResult;
      this.followupTask = followupTask;
    }

    /*Nullable*/ public R getPartialResult() {
      return partialResult;
    }

    /*Nullable*/ public T getFollowupTask() {
      return followupTask;
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "("
          + partialResult + ", "
          + followupTask
          + ")";
    }
  }

  /**
   * Runs this task, returning a {@link RunResult}.
   *
   * May be invoked multiple times, possibly concurrently.
   *
   * If this throws an exception, it may be retried a limited number of times
   * according to an unspecified retry policy.
   */
  RunResult<T, R> run();

}
