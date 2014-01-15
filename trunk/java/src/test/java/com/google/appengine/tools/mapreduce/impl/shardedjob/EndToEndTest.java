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
        new TestTask(0, 5, 1, 3),
        new TestTask(1, 5, 10, 1),
        new TestTask(2, 5, 100, 1),
        new TestTask(3, 5, 1000, 2),
        new TestTask(4, 5, 10000, 4));
    int expectedResult = 42113;

    String jobId = "job1";
    assertNull(service.getJobState(jobId));

    assertEquals(0, getTasks().size());
    TestController controller = new TestController(expectedResult);
    service.startJob(jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), service.getJobState(jobId).getStatus());
    // 5 initial tasks
    assertEquals(5, getTasks().size());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    // Starting again should not add any tasks.
    controller = new TestController(expectedResult);
    service.startJob(jobId, tasks, controller, settings);
    assertEquals(new Status(RUNNING), service.getJobState(jobId).getStatus());
    assertEquals(5, getTasks().size());
    assertEquals(5, service.getJobState(jobId).getActiveTaskCount());
    assertEquals(5, service.getJobState(jobId).getTotalTaskCount());

    executeTasksUntilEmpty();

    ShardedJobState<?> state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).
  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(service.getJobState(jobId));
    TestController controller = new TestController(0);
    service.startJob(jobId, ImmutableList.<TestTask>of(), controller, settings);
    ShardedJobState<?> state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }
}
