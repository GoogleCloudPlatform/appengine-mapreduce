// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class EndToEndTest extends EndToEndTestCase {

  private static class Task implements IncrementalTask<Task, Integer> {
    private final int result;
    private final Task followupTask;

    Task(int result, Task followupTask) {
      this.result = result;
      this.followupTask = followupTask;
    }

    @Override public RunResult<Task, Integer> run() {
      return RunResult.of(result, followupTask);
    }
  }

  private static class Controller implements ShardedJobController<Task, Integer> {
    private final int expectedResult;

    Controller(int expectedResult) {
      this.expectedResult = expectedResult;
    }

    @Override public Integer combineResults(Iterable<Integer> inputs) {
      int sum = 0;
      for (Integer x : inputs) {
        sum += x;
      }
      return sum;
    }

    // TODO(ohler): Assert that this actually gets called.  Seems likely that
    // will be covered by higher-level end-to-end tests, though.
    @Override public void completed(Integer finalCombinedResult) {
      assertEquals(Integer.valueOf(expectedResult), finalCombinedResult);
    }
  }

  public void testSimpleJob() throws Exception {
    List<Task> tasks = ImmutableList.of(
        new Task(1, new Task(10, new Task(100, null))),
        new Task(1000, null),
        new Task(10000, null),
        new Task(100000, new Task(1000000, null)),
        new Task(10000000, new Task(100000000, new Task(1000000000, null))));
    int expectedResult = 1111111111;

    String jobId = "job1";
    assertNull(service.getJobState(jobId));

    assertEquals(0, getTasks().size());
    service.startJob(jobId, tasks, new Controller(expectedResult), settings);
    assertEquals(Status.RUNNING, service.getJobState(jobId).getStatus());
    // 5 initial tasks plus controller.
    assertEquals(6, getTasks().size());
    assertEquals(0, service.getJobState(jobId).getAggregateResult());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    // Starting again should not add any tasks.
    service.startJob(jobId, tasks, new Controller(expectedResult), settings);
    assertEquals(Status.RUNNING, service.getJobState(jobId).getStatus());
    assertEquals(6, getTasks().size());
    assertEquals(0, service.getJobState(jobId).getAggregateResult());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    executeTasksUntilEmpty();

    ShardedJobState<?, ?> state = service.getJobState(jobId);
    assertEquals(Status.DONE, state.getStatus());
    assertEquals(expectedResult, state.getAggregateResult());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).

  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(service.getJobState(jobId));
    service.startJob(jobId, ImmutableList.<Task>of(), new Controller(0), settings);
    ShardedJobState<?, ?> state = service.getJobState(jobId);
    assertEquals(Status.DONE, state.getStatus());
    assertEquals(0, state.getAggregateResult());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }

}
