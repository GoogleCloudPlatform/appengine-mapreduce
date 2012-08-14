// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.util.List;

/**
 * Runs a sharded job in the current process.  Only for very small jobs.  Easier
 * to debug than a parallel execution.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class InProcessShardedJobRunner {

  private InProcessShardedJobRunner() {
  }

  /**
   * Runs the given job and returns its result.
   */
  public static <T extends IncrementalTask<T, R>, R extends Serializable> R runJob(
      List<? extends T> initialTasks, ShardedJobController<T, R> controller) {
    R partialResult = controller.combineResults(ImmutableList.<R>of());
    for (T task : initialTasks) {
      Preconditions.checkNotNull(task, "Null initial task: %s", initialTasks);
      do {
        IncrementalTask.RunResult<T, R> runResult = task.run();
        partialResult = controller.combineResults(
            ImmutableList.of(partialResult, runResult.getPartialResult()));
        task = runResult.getFollowupTask();
      } while (task != null);
    }
    controller.completed(partialResult);
    return partialResult;
  }

}
