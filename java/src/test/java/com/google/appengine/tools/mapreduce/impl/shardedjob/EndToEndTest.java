// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class EndToEndTest extends EndToEndTestCase {

  private final ShardedJobService service = ShardedJobServiceFactory.getShardedJobService();
  private ShardedJobSettings settings;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    settings = new ShardedJobSettings.Builder().build();
  }

  @Test
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

    ShardedJobState state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(5, state.getTotalTaskCount());
  }


  private static class TestController1 extends TestController {
    private static final long serialVersionUID = 8297824686146604329L;

    public TestController1(int expectedResult) {
      super(expectedResult);
    }

    @Override
    public void completed(Iterator<TestTask> results) {
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      assertEquals(7, ds.prepare(new Query()).countEntities(FetchOptions.Builder.withDefaults()));
      super.completed(results);
    }
  }

  @Test
  public void testJobFinalization() throws Exception {
    byte[] bytes = new byte[1024 * 1024];
    new Random().nextBytes(bytes);
    TestTask task = new TestTask(0, 1, 1, 1, bytes);
    String jobId = "job1";
    TestController controller = new TestController1(1);
    service.startJob(jobId, ImmutableList.of(task), controller, settings);
    assertEquals(new Status(RUNNING), service.getJobState(jobId).getStatus());
    executeTasksUntilEmpty();
    ShardedJobState state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    IncrementalTaskState<IncrementalTask> it = Iterators.getOnlyElement(service.lookupTasks(state));
    assertNull(((TestTask) it.getTask()).getPayload());
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    assertEquals(2, ds.prepare(new Query()).countEntities(FetchOptions.Builder.withDefaults()));
  }

  // TODO(ohler): Test idempotence of startJob() in more depth, especially in
  // the case of errors (incomplete initializations).
  @Test
  public void testNoTasks() throws Exception {
    String jobId = "job1";
    assertNull(service.getJobState(jobId));
    TestController controller = new TestController(0);
    service.startJob(jobId, ImmutableList.<TestTask>of(), controller, settings);
    ShardedJobState state = service.getJobState(jobId);
    assertEquals(new Status(DONE), state.getStatus());
    assertEquals(0, state.getActiveTaskCount());
    assertEquals(0, state.getTotalTaskCount());
  }
}
