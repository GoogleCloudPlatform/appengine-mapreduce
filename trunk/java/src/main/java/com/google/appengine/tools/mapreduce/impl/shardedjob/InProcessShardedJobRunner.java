// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
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
  public static <T extends IncrementalTask> void runJob(
      List<T> initialTasks, ShardedJobController<T> controller) {
    List<T> results = new ArrayList<>();
    for (T task : initialTasks) {
      Preconditions.checkNotNull(task, "Null initial task: %s", initialTasks);
      do {
        task.run();
      } while (!task.isDone());
      results.add(task);
    }
    controller.completed(results);
  }
}
