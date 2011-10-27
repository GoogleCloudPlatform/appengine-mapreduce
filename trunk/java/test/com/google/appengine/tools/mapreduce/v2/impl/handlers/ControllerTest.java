// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.HadoopCounterNames;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.MockClock;
import com.google.appengine.tools.mapreduce.QuotaManager;
import com.google.appengine.tools.mapreduce.StubInputFormat;
import com.google.appengine.tools.mapreduce.StubInputSplit;
import com.google.appengine.tools.mapreduce.StubMapper;
import com.google.appengine.tools.mapreduce.StubRecordReader;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.easymock.EasyMock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */
public class ControllerTest extends TestCase {
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

    private MapReduceServlet servlet;

    private DatastoreService ds;

    @Override
    public void setUp() throws Exception {
      super.setUp();
      helper.setUp();
      ds = DatastoreServiceFactory.getDatastoreService();
      servlet = new MapReduceServlet();
      StubMapper.fakeSetUp();
    }

    @Override
    public void tearDown() throws Exception {
      StubMapper.fakeTearDown();
      helper.tearDown();
      super.tearDown();
    }

    private static HttpServletRequest createMockRequest(
        String handler, boolean taskQueueRequest, boolean ajaxRequest) {
      HttpServletRequest request = createMock(HttpServletRequest.class);
      if (taskQueueRequest) {
        expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn("default")
          .anyTimes();
      } else {
        expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn(null)
          .anyTimes();
      }
      if (ajaxRequest) {
        expect(request.getHeader("X-Requested-With"))
          .andReturn("XMLHttpRequest")
          .anyTimes();
      } else {
        expect(request.getHeader("X-Requested-With"))
          .andReturn(null)
          .anyTimes();
      }
      expect(request.getRequestURI())
        .andReturn("/mapreduce/" + handler)
        .anyTimes();
      return request;
    }

    private static HttpServletRequest createMockStartRequest(Configuration conf) {
      HttpServletRequest request = createMockRequest(MapReduceServlet.START_PATH, false, false);
      expect(request.getParameter(AppEngineJobContext.CONFIGURATION_PARAMETER_NAME))
        .andReturn(ConfigurationXmlUtil.convertConfigurationToXml(conf))
        .anyTimes();
      return request;
    }

    private static HttpServletRequest createMockControllerRequest(int sliceNumber, JobID jobId) {
      HttpServletRequest request = createMockRequest(MapReduceServlet.CONTROLLER_PATH, true, false);
      expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
        .andReturn("" + sliceNumber)
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn("" + jobId)
        .anyTimes();
      return request;
    }

    public void testControllerCSRF() {
      JobID jobId = new JobID("foo", 1);
      // Send it as an AJAX request but not a task queue request - should be denied.
      HttpServletRequest request = createMockRequest(MapReduceServlet.CONTROLLER_PATH, false, true);
      expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
        .andReturn("" + 0)
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn("" + jobId)
        .anyTimes();
      HttpServletResponse response = createMock(HttpServletResponse.class);
      try {
        response.sendError(403, "Received unexpected non-task queue request.");
      } catch (IOException ioe) {
        // Can't actually be sent in mock setup
      }
      replay(request, response);
      servlet.doPost(request, response);
      verify(request, response);
    }

    public void testGetJobDetailCSRF() {
      JobID jobId = new JobID("foo", 1);
      // Send it as a task queue request but not an ajax request - should be denied.
      HttpServletRequest request = createMockRequest(
          MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.GET_JOB_DETAIL_PATH, true, false);
      expect(request.getMethod())
        .andReturn("POST")
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn("" + jobId)
        .anyTimes();

      HttpServletResponse response = createMock(HttpServletResponse.class);

      // Set before error and last one wins, so this is harmless.
      response.setContentType("application/json");
      EasyMock.expectLastCall().anyTimes();

      try {
        response.sendError(403, "Received unexpected non-XMLHttpRequest command.");
      } catch (IOException ioe) {
        // Can't actually be sent in mock setup
      }
      replay(request, response);
      servlet.doGet(request, response);
      verify(request, response);
    }

    private static Configuration getSampleMapReduceConfiguration() {
      Configuration conf = new Configuration(false);
      // TODO(user): If I can find a way to keep the test small
      // I'd like to exercise the non-default queues, but currently
      // it looks like the test harness only supports an actual queues.xml.
      conf.set(AppEngineJobContext.CONTROLLER_QUEUE_KEY, "default");
      conf.set(AppEngineJobContext.WORKER_QUEUE_KEY, "default");
      conf.set(AppEngineJobContext.MAPPER_SHARD_COUNT_KEY, "2");
      conf.set(AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY, "1000");
      conf.setClass("mapreduce.inputformat.class", StubInputFormat.class, InputFormat.class);
      conf.setClass("mapreduce.map.class", StubMapper.class, AppEngineMapper.class);
      return conf;
    }

    // Creates an MR state with an empty configuration and the given job ID,
    // and stores it in the datastore.
    private void persistMRState(JobID jobId, Configuration conf) {
      MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(ds, "", jobId, 0);
      mrState.setConfigurationXML(ConfigurationXmlUtil.convertConfigurationToXml(conf));
      mrState.persist();
    }

      private static QueueStateInfo getDefaultQueueInfo() {
      LocalTaskQueue localQueue
          = (LocalTaskQueue) LocalServiceTestHelper.getLocalService(
              LocalTaskQueue.PACKAGE);
      QueueStateInfo defaultQueue = localQueue.getQueueStateInfo().get("default");
      return defaultQueue;
    }

    public void testBailsOnBadHandler() {
      HttpServletRequest request = createMockRequest("fizzle", true, true);
      HttpServletResponse response = createMock(HttpServletResponse.class);
      replay(request, response);
      try {
        servlet.doPost(request, response);
        fail("Should have thrown RuntimeException");
      } catch (RuntimeException e) {
        // Pass
      }
      verify(request, response);
    }

    /**
     * Tests that quota refills stop if there are no active shards left.
     */
    public void testRefillQuotas_noneActive() {
      List<ShardState> activeShardStates = new ArrayList<ShardState>();
      // With all the nulls this should bomb horribly if the first check doesn't
      // catch this.
      Controller.refillQuotas(null, null, activeShardStates);
    }

    public void testRefillQuotas() {
      Configuration conf = new Configuration(false);
      conf.setInt(AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY, 1000);

      JobID jobId = new JobID("foo", 1);

      // Set up a MapReduceState that includes a processing rate of 1000
      // inputs/sec
      MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(
          ds, "", jobId, 0);

      long startTime = 123;
      mrState.setLastPollTime(startTime);

      mrState.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(conf));
      mrState.persist();

      HttpServletRequest req = createMock(HttpServletRequest.class);
      expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
          .andReturn(jobId.toString())
          .anyTimes();

      replay(req);

      AppEngineJobContext context = new AppEngineJobContext(req);

      MockClock clock = new MockClock(startTime + 1000);
      Controller.clock = clock;

      // There are two active shards.
      List<ShardState> shardStates = Arrays.asList(
          ShardState.generateInitializedShardState(ds,
              new TaskAttemptID(new TaskID(jobId, true, 1), 1)),
          ShardState.generateInitializedShardState(ds,
              new TaskAttemptID(new TaskID(jobId, true, 2), 1)));

      // Actually attempt to refill quotas
      Controller.refillQuotas(context, mrState, shardStates);

      // Check that everything went as planned
      long endTime = mrState.getLastPollTime();
      assertTrue("Mock time should have been used.",
          endTime == startTime + 1000);
      double totalQuota = 1000.0 * (endTime - startTime) / 1000;
      QuotaManager manager = new QuotaManager(
          MemcacheServiceFactory.getMemcacheService());

      // The quota should have been divided equally between the two shards.
      // (500 quota for each)
      assertEquals(totalQuota / 2,
          manager.get(shardStates.get(0).getTaskAttemptID().toString()), 1.0);

      verify(req);
    }

    /**
     * Returns a sorted list of all counter names in a counter group.
     */
    private static List<String> getCounterGroupNamesAsList(Counters aggregateCounters,
        String counterGroupName) {
      Iterator<Counter> counterGroupCountersIt =
          aggregateCounters.getGroup(counterGroupName).iterator();
      List<String> counterGroupNames = new ArrayList<String>();
      while (counterGroupCountersIt.hasNext()) {
        counterGroupNames.add(counterGroupCountersIt.next().getName());
      }
      Collections.sort(counterGroupNames);
      return counterGroupNames;
    }

    public void testAggregateState() {
      TaskAttemptID taskId1 = new TaskAttemptID(
          new TaskID(new JobID("foo", 1), true, 1), 1);
      ShardState shardState1 = ShardState.generateInitializedShardState(
          ds, taskId1);
      Counters counters1 = shardState1.getCounters();
      counters1.findCounter("binky", "winky").increment(10);
      counters1.findCounter("binky", "slinky").increment(1);
      counters1.findCounter(HadoopCounterNames.MAP_INPUT_RECORDS_GROUP,
          HadoopCounterNames.MAP_INPUT_RECORDS_NAME).increment(5);
      shardState1.setCounters(counters1);

      TaskAttemptID taskId2 = new TaskAttemptID(
          new TaskID(new JobID("foo", 1), true, 2), 1);
      ShardState shardState2 = ShardState.generateInitializedShardState(
          ds, taskId2);
      Counters counters2 = shardState2.getCounters();
      counters2.findCounter("binky", "slinky").increment(3);
      counters2.findCounter("zinky", "whee").increment(7);
      shardState2.setCounters(counters2);

      MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(
          ds, "", taskId1.getJobID(), 0);
      Controller.aggregateState(mrState, Arrays.asList(shardState1, shardState2));
      Counters aggregateCounters = mrState.getCounters();

      List<String> sortedCounterNames = new ArrayList<String>(aggregateCounters.getGroupNames());
      Collections.sort(sortedCounterNames);
      assertEquals(Arrays.asList("binky", HadoopCounterNames.MAP_INPUT_RECORDS_GROUP, "zinky"),
          sortedCounterNames);

      assertEquals(Arrays.asList("slinky", "winky"),
          getCounterGroupNamesAsList(aggregateCounters, "binky"));

      assertEquals(Arrays.asList(HadoopCounterNames.MAP_INPUT_RECORDS_NAME),
          getCounterGroupNamesAsList(aggregateCounters, HadoopCounterNames.MAP_INPUT_RECORDS_GROUP));

      assertEquals(Arrays.asList("whee"),
          getCounterGroupNamesAsList(aggregateCounters, "zinky"));

      // 10 from shard 1
      assertEquals(10, aggregateCounters.findCounter("binky", "winky").getValue());
      // 7 from shard 2
      assertEquals(7, aggregateCounters.findCounter("zinky", "whee").getValue());

      // 1 from shard 1, 3 from shard 2
      assertEquals(4, aggregateCounters.findCounter("binky", "slinky").getValue());

      assertTrue("Unexpected chart URL: " + mrState.getChartUrl(),
          mrState.getChartUrl().contains("google.com"));
    }

    /**
     * Test that handleController has reasonable behavior when there are still
     * active workers.
     *
     * @throws EntityNotFoundException
     */
    public void testHandleController_withContinue() throws EntityNotFoundException {
      JobID jobId = new JobID("foo", 1);
      HttpServletRequest request = createMockControllerRequest(0, jobId);
      replay(request);

      Configuration sampleConf = getSampleMapReduceConfiguration();

      persistMRState(jobId, sampleConf);

      ShardState shardState1 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(jobId, true, 1), 1));
      Counters counters1 = new Counters();
      counters1.findCounter("a", "z").increment(1);
      shardState1.setCounters(counters1);
      shardState1.setInputSplit(sampleConf, new StubInputSplit(1));
      shardState1.setRecordReader(sampleConf, new StubRecordReader());
      shardState1.persist();

      ShardState shardState2 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(jobId, true, 2), 1));
      Counters counters2 = new Counters();
      counters2.findCounter("a", "z").increment(1);
      shardState2.setCounters(counters2);
      shardState2.setInputSplit(sampleConf, new StubInputSplit(2));
      shardState2.setRecordReader(sampleConf, new StubRecordReader());
      shardState2.setDone();
      shardState2.persist();

      // doPost should call handleCallback()
      // resp is never used
      servlet.doPost(request, null);

      MapReduceState mrState = MapReduceState.getMapReduceStateFromJobID(ds, jobId);

      // Check result of aggregateState()
      assertEquals(2, mrState.getCounters().findCounter("a", "z").getValue());

      // Check the result of refillQuota()
      // Should fill the active thread but not the done one.
      assertEquals(1000,
          new QuotaManager(MemcacheServiceFactory.getMemcacheService()).get(
              "" + shardState1.getTaskAttemptID()));
      assertEquals(0,
          new QuotaManager(MemcacheServiceFactory.getMemcacheService()).get(
              "" + shardState2.getTaskAttemptID()));

      // Check that the next controller task got enqueued.
      QueueStateInfo defaultQueue = getDefaultQueueInfo();
      assertEquals(1, defaultQueue.getCountTasks());
      TaskStateInfo firstTask = defaultQueue.getTaskInfo().get(0);
      assertEquals("/mapreduce/" + MapReduceServlet.CONTROLLER_PATH, firstTask.getUrl());
      assertTrue(firstTask.getBody(),
          firstTask.getBody().indexOf("jobID=job_foo_0001") != -1);

      assertEquals(1, mrState.getActiveShardCount());
      assertEquals(2, mrState.getShardCount());

      verify(request);
    }

    /**
     * Test that the controller cleans up inactive shard states.
     */
    public void testHandleController_withCleanup() {
      JobID jobId = new JobID("foo", 1);
      HttpServletRequest request = createMockControllerRequest(0, jobId);
      replay(request);

      Configuration sampleConf = getSampleMapReduceConfiguration();
      persistMRState(jobId, sampleConf);

      ShardState shardState1 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(jobId, true, 1), 1));
      Counters counters1 = new Counters();
      counters1.findCounter("a", "z").increment(1);
      shardState1.setCounters(counters1);
      shardState1.setInputSplit(sampleConf, new StubInputSplit(1));
      shardState1.setRecordReader(sampleConf, new StubRecordReader());
      shardState1.setDone();
      shardState1.persist();

      Controller.handleController(request);

      assertFalse("Controller should clean up all shard states.",
          ds.prepare(new Query("ShardState")).asIterator().hasNext());

      assertEquals("Controller shouldn't reschedule after all shards are done.",
          0, getDefaultQueueInfo().getCountTasks());
      verify(request);
    }

    /**
     * Test that the controller schedules done callback.
     */
    public void testHandleController_withDoneCallback() {
      JobID jobId = new JobID("foo", 1);
      HttpServletRequest request = createMockControllerRequest(0, jobId);
      replay(request);

      Configuration sampleConf = getSampleMapReduceConfiguration();
      sampleConf.set(AppEngineJobContext.DONE_CALLBACK_URL_KEY, "/foo/bar");
      persistMRState(jobId, sampleConf);

      ShardState shardState1 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(jobId, true, 1), 1));
      Counters counters1 = new Counters();
      counters1.findCounter("a", "z").increment(1);
      shardState1.setCounters(counters1);
      shardState1.setInputSplit(sampleConf, new StubInputSplit(1));
      shardState1.setRecordReader(sampleConf, new StubRecordReader());
      shardState1.setDone();
      shardState1.persist();

      Controller.handleController(request);

      assertEquals("Controller should add done callback.",
          1, getDefaultQueueInfo().getCountTasks());
      TaskStateInfo taskInfo =getDefaultQueueInfo().getTaskInfo().get(0);
      assertEquals("Controller should add done callback url.",
          "/foo/bar", taskInfo.getUrl());
      assertTrue("Controller should pass the job ID to the done callback. Got: "
           + taskInfo.getBody(), taskInfo.getBody().contains("job_foo_0001"));
      verify(request);
    }

    public void testHandleStart() {
      HttpServletRequest request = createMockStartRequest(getSampleMapReduceConfiguration());
      replay(request);

      servlet.doPost(request, null);

      QueueStateInfo defaultQueue = getDefaultQueueInfo();

      int workerCount = 0;
      int controllerCount = 0;

      for (TaskStateInfo task : defaultQueue.getTaskInfo()) {
        if (task.getUrl().endsWith("/mapperCallback")) {
          assertTrue("Worker task has not task attempt ID: " + task.getBody(),
              task.getBody().indexOf("taskAttemptID=") != -1);
          workerCount++;
        } else if (task.getUrl().endsWith("/controllerCallback")) {
          controllerCount++;
          assertTrue("Controller task has no job ID: " + task.getBody(),
              task.getBody().indexOf("jobID=") != -1);
        } else {
          fail("Got unknown enqueued task: " + task.getUrl());
        }
      }

      assertEquals(2, workerCount);
      assertEquals(1, controllerCount);
      verify(request);
    }

}
