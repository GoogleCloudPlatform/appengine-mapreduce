// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class EndToEndTest extends EndToEndTestCase {

  public void testSimpleJob() throws Exception {
    List<TestTask> tasks = ImmutableList.of(
        new TestTask(1, new TestTask(10, new TestTask(100, null))),
        new TestTask(1000, null),
        new TestTask(10000, null),
        new TestTask(100000, new TestTask(1000000, null)),
        new TestTask(10000000, new TestTask(100000000, new TestTask(1000000000, null))));
    int expectedResult = 1111111111;

    String jobId = "job1";
    assertNull(service.getJobState(jobId));

    assertEquals(0, getTasks().size());
    service.startJob(jobId, tasks, new TestController(expectedResult), settings);
    assertEquals(new Status(RUNNING), service.getJobState(jobId).getStatus());
    // 5 initial tasks plus controller.
    assertEquals(6, getTasks().size());
    assertEquals(0, service.getJobState(jobId).getAggregateResult());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    // Starting again should not add any tasks.
    service.startJob(jobId, tasks, new TestController(expectedResult), settings);
    assertEquals(new Status(RUNNING), service.getJobState(jobId).getStatus());
    assertEquals(6, getTasks().size());
    assertEquals(0, service.getJobState(jobId).getAggregateResult());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    executeTasksUntilEmpty();

    ShardedJobState<?, ?> state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(expectedResult, state.getAggregateResult());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).

  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(service.getJobState(jobId));
    service.startJob(jobId, ImmutableList.<TestTask>of(), new TestController(0), settings);
    ShardedJobState<?, ?> state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getAggregateResult());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }
}
