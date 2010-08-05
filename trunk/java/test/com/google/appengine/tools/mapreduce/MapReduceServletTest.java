/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.labs.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.labs.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.labs.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.easymock.EasyMock;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests MapReduceServlet
 * 
 * @author frew@google.com (Fred Wulff)
 */
public class MapReduceServletTest extends TestCase {
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
      
  public void testGetHandler() {
    HttpServletRequest req = createMockStartRequest(new Configuration(false));
    replay(req);
    
    assertEquals("start", MapReduceServlet.getHandler(req));
    verify(req);
  }
  
  private HttpServletRequest createMockRequest(
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
  
  private HttpServletRequest createMockStartRequest(Configuration conf) {
    HttpServletRequest request = createMockRequest(MapReduceServlet.START_PATH, false, false);
    expect(request.getParameter(AppEngineJobContext.CONFIGURATION_PARAMETER_NAME))
      .andReturn(ConfigurationXmlUtil.convertConfigurationToXml(conf))
      .anyTimes();
    return request;
  }
  
  private HttpServletRequest createMockStartJobRequest(Configuration conf) {
    HttpServletRequest request = createMockRequest(
        MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.START_JOB_PATH, false, true);
    expect(request.getMethod())
      .andReturn("POST")
      .anyTimes();
    return request;
  }
  
  private HttpServletRequest createMockControllerRequest(int sliceNumber, JobID jobId) {
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

  private HttpServletRequest createMockMapperWorkerRequest(int sliceNumber,
      JobID jobId, TaskAttemptID taskAttemptId) {
    HttpServletRequest request = createMockRequest(MapReduceServlet.CONTROLLER_PATH, true, false);
    expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
      .andReturn("" + sliceNumber)
      .anyTimes();
    expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
      .andReturn(jobId.toString())
      .anyTimes();
    expect(request.getParameter(AppEngineTaskAttemptContext.TASK_ATTEMPT_ID_PARAMETER_NAME))
      .andReturn(taskAttemptId.toString())
      .anyTimes();
    return request;
  }
  
  public void testGetBase() {
    HttpServletRequest request = createMockStartRequest(new Configuration(false));
    replay(request);
    
    assertEquals("/mapreduce/", MapReduceServlet.getBase(request));
    verify(request);
  }
  
  public void testGetCommandBaseAndHandler() {
    HttpServletRequest request = createMockStartJobRequest(new Configuration(false));
    replay(request);
    
    assertEquals("/mapreduce/", MapReduceServlet.getBase(request));
    // Command should be treated as part of the handler
    assertEquals("command/start_job", MapReduceServlet.getHandler(request));
    verify(request);
  }
    
  private Configuration getSampleMapReduceConfiguration() {
    Configuration conf = new Configuration(false);
    // TODO(frew): If I can find a way to keep the test small
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
  
  public void testScheduleShards() {
    JobID jobId = new JobID("123", 1);
    HttpServletRequest request = createMockStartRequest(getSampleMapReduceConfiguration());
    expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
      .andReturn(jobId.toString())
      .anyTimes();
    replay(request);
    
    Configuration conf = getSampleMapReduceConfiguration();
    persistMRState(jobId, conf);
    AppEngineJobContext context = new AppEngineJobContext(request, false);
    List<InputSplit> splits = Arrays.asList(
        (InputSplit) new StubInputSplit(1), new StubInputSplit(2));
    StubInputFormat format = new StubInputFormat();
    servlet.scheduleShards(request, context, format, splits);
    QueueStateInfo defaultQueue = getDefaultQueueInfo();
    assertEquals(2, defaultQueue.getCountTasks());
    TaskStateInfo firstTask = defaultQueue.getTaskInfo().get(0);
    assertEquals("/mapreduce/mapperCallback", firstTask.getUrl());
    assertTrue(firstTask.getBody(), 
        firstTask.getBody().indexOf("taskAttemptID=attempt_123_0001_m_000000") != -1);
    verify(request);
  }

  private QueueStateInfo getDefaultQueueInfo() {
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
    servlet.refillQuotas(null, null, activeShardStates);
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
    
    AppEngineJobContext context = new AppEngineJobContext(req, false);
    
    MockClock clock = new MockClock(startTime + 1000);
    servlet.setClock(clock);
    
    // There are two active shards.
    List<ShardState> shardStates = Arrays.asList(
        ShardState.generateInitializedShardState(ds, 
            new TaskAttemptID(new TaskID(jobId, true, 1), 1)),
        ShardState.generateInitializedShardState(ds, 
            new TaskAttemptID(new TaskID(jobId, true, 2), 1)));
    
    // Actually attempt to refill quotas
    servlet.refillQuotas(context, mrState, shardStates);
    
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
  private List<String> getCounterGroupNamesAsList(Counters aggregateCounters,
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
    servlet.aggregateState(mrState, Arrays.asList(shardState1, shardState2));
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
    
    servlet.handleController(request, null);
    
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
    
    servlet.handleController(request, null);
    
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
  
  /**
   * Test that a mapper worker behaves gracefully without quota.
   */
  public void testMapperWorker_withoutQuota() throws IOException {
    JobID jobId = new JobID("foo", 1);
    TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(jobId, true, 0), 0);
    
    HttpServletRequest request = createMockMapperWorkerRequest(0, jobId, taskAttemptId);
    
    replay(request);
    
    Configuration conf = getSampleMapReduceConfiguration();
    persistMRState(jobId, conf);
    
    ShardState state = ShardState.generateInitializedShardState(ds, taskAttemptId);
    StubInputFormat format = new StubInputFormat();
    AppEngineJobContext context = new AppEngineJobContext(conf, jobId, request);
    
    AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
        context, state, taskAttemptId);
    List<InputSplit> splits = format.getSplits(context);
    state.setInputSplit(conf, splits.get(0));
    RecordReader<IntWritable, IntWritable> reader = format.createRecordReader(
        splits.get(0), taskAttemptContext);
    state.setRecordReader(conf, reader);
    state.persist();
    servlet.handleMapperWorker(request, null);
    
    // No quota so shouldn't invoke any maps.
    assertEquals(0, StubMapper.invocationKeys.size());
    
    QueueStateInfo defaultQueue = getDefaultQueueInfo();
    assertEquals(1, defaultQueue.getCountTasks());
    TaskStateInfo firstTask = defaultQueue.getTaskInfo().get(0);
    assertEquals("/mapreduce/mapperCallback", firstTask.getUrl());
    assertTrue(firstTask.getBody(), 
        firstTask.getBody().indexOf("sliceNumber=1") != -1);
    
    assertTrue(StubMapper.setupCalled);
    assertFalse(StubMapper.cleanupCalled);
    
    // Per task handler shouldn't be called if there's no quota to begin with.
    assertFalse(StubMapper.taskSetupCalled);
    assertFalse(StubMapper.taskCleanupCalled);
  
    verify(request);
  }
  
  public void testMapperWorker_withQuota() {
    JobID jobId = new JobID("foo", 1);
    TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(jobId, true, 0), 0);
    
    HttpServletRequest request = createMockMapperWorkerRequest(0, jobId, taskAttemptId);
    
    replay(request);
    
    Configuration conf = getSampleMapReduceConfiguration();
    persistMRState(jobId, conf);
    
    ShardState state = ShardState.generateInitializedShardState(ds, taskAttemptId);
    StubInputFormat format = new StubInputFormat();
    AppEngineJobContext context = new AppEngineJobContext(conf, jobId, request);
    
    AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
        context, state, taskAttemptId);
    List<InputSplit> splits = format.getSplits(context);
    state.setInputSplit(conf, splits.get(0));
    RecordReader<IntWritable, IntWritable> reader = format.createRecordReader(
        splits.get(0), taskAttemptContext);
    state.setRecordReader(conf, reader);
    state.persist();
    new QuotaManager(MemcacheServiceFactory.getMemcacheService()).put(
        taskAttemptId.toString(), 10);
    servlet.handleMapperWorker(request, null);
    assertEquals(((StubInputSplit) splits.get(0)).getKeys(), 
        StubMapper.invocationKeys); 
    verify(request);
  }
  
  // Ensures that being out of quota on the consume call doesn't
  // skip entities.
  @SuppressWarnings("unchecked")
  public void testProcessMapper_noQuotaDoesntSkip() throws Exception {
    AppEngineMapper mapper = createMock(AppEngineMapper.class);
    QuotaConsumer consumer = createMock(QuotaConsumer.class);
    
    // Process mapper should both start and cleanup the task,
    // but shouldn't actually get entities because it's out of quota.
    mapper.taskSetup(null);
    mapper.taskCleanup(null);
    
    // Simulate having quota to start and then running out
    expect(consumer.check(1)).andReturn(true);
    expect(consumer.consume(1)).andReturn(false);
    replay(mapper, consumer);
    
    servlet.processMapper(mapper, null, consumer, System.currentTimeMillis() + 10000);
    
    verify(mapper, consumer);
  }
  
  /**
   * Compares a string representation of the expected JSON object
   * with the actual, ignoring white space and converting single quotes
   * to double quotes.
   */
  public void assertJsonEquals(String expected, JSONObject actual) {
    assertEquals(expected.replace('\'', '"').replace(" ", ""), 
        actual.toString().replace(" ", "").replace("\\n", ""));
  }
  
  public void testHandleListConfigs() throws Exception {
    InputStream testStream = new ByteArrayInputStream(
        MapReduceXmlTest.SAMPLE_CONFIGURATION_XML.getBytes());
    JSONObject retValue = servlet.handleListConfigs(new MapReduceXml(testStream));
    assertJsonEquals(
          "{'configs':["
        + "  {'mapper_params':"
        + "    {'Baz':"
        + "      {'default_value':'2','human_name':'Baz'}"
        + "    },"
        + "   'name':'Bar'"
        + "  }," 
        + "  {'mapper_params':{},"
        + "   'name':'Foo'"
        + "  }"
        + "]}", retValue);
  }
  
  // Tests list jobs by getting two batches of two jobs
  public void testHandleListJobs() throws Exception {
    for (int i = 0; i < 4; i++) {
      MapReduceState state = MapReduceState.generateInitializedMapReduceState(
          ds, "", new JobID("" + i, i), 0);
      state.setConfigurationXML("<configuration></configuration>");
      state.setLastPollTime(i);
      state.persist();
    }

    JSONObject retObject = servlet.handleListJobs(null, 2);
    assertJsonEquals(
          "{'jobs':["
        + "  {"
        + "   'mapreduce_id':'job_0_0000',"
        + "   'shards':0,"
        + "   'name':'',"
        + "   'active':true,"
        + "   'active_shards':0,"
        + "   'updated_timestamp_ms':0,"
        + "   'start_timestamp_ms':0},"
        + "  {'mapreduce_id':'job_1_0001',"
        + "   'shards':0,"
        + "   'name':''," 
        + "   'active':true," 
        + "   'active_shards':0,"
        + "   'updated_timestamp_ms':1,"
        + "   'start_timestamp_ms':0}],"
        + " 'cursor':'E9oBQWo8agR0ZXN0cjQLEhIKABoOTWFwUmVkdWNlU3RhdGUMCxIOTWFwUmVkdWNlU3RhdGUiCmpvY"
        +            "l8xXzAwMDEMggEA4AEAFA'"
        + "}", retObject);
      
    // Get a second batch of jobs. There should only be 2 left after the cursor
    // despite the high limit.
    retObject = servlet.handleListJobs(retObject.getString("cursor"), 50);
    assertJsonEquals(
          "{'jobs':[{'mapreduce_id':'job_2_0002',"
        + "          'shards':0,"
        + "          'name':'',"
        + "          'active':true,"
        + "          'active_shards':0,"
        + "          'updated_timestamp_ms':2,"
        + "          'start_timestamp_ms':0},"
        + "         {'mapreduce_id':'job_3_0003',"
        + "          'shards':0,"
        + "          'name':'',"
        + "          'active':true,"
        + "          'active_shards':0,"
        + "          'updated_timestamp_ms':3,"
        + "          'start_timestamp_ms':0}],"
        + " 'cursor':'E9oBQWo8agR0ZXN0cjQLEhIKABoOTWFwUmVkdWNlU3RhdGUMCxIOTWFwUmVkdWNlU3RhdGUiCmpvY"
        +            "l8zXzAwMDMMggEA4AEAFA'}"
        , retObject);
  }
  
  public void testStaticResources_status() throws Exception {
    HttpServletResponse resp = createMock(HttpServletResponse.class);
    resp.setContentType("text/html");
    resp.setHeader("Cache-Control", "public; max-age=300");
    ServletOutputStream sos = createMock(ServletOutputStream.class);
    expect(resp.getOutputStream()).andReturn(sos);
    sos.write((byte[]) EasyMock.anyObject(), EasyMock.eq(0), EasyMock.anyInt());
    EasyMock.expectLastCall().atLeastOnce();
    sos.flush();
    EasyMock.expectLastCall().anyTimes();
    replay(resp, sos);
    servlet.handleStaticResources("status", resp);
    verify(resp, sos);
  }

  public void testStaticResources_jQuery() throws Exception {
    HttpServletResponse resp = createMock(HttpServletResponse.class);
    resp.setContentType("text/javascript");
    resp.setHeader("Cache-Control", "public; max-age=300");
    ServletOutputStream sos = createMock(ServletOutputStream.class);
    expect(resp.getOutputStream()).andReturn(sos);
    sos.write((byte[]) EasyMock.anyObject(), EasyMock.eq(0), EasyMock.anyInt());
    EasyMock.expectLastCall().atLeastOnce();
    sos.flush();
    EasyMock.expectLastCall().anyTimes();
    replay(resp, sos);
    servlet.handleStaticResources("jquery.js", resp);
    verify(resp, sos);
  }

  // Tests that an job that has just been initialized returns a reasonable
  // job detail.
  public void testGetJobDetail_empty() throws Exception {
    MapReduceState state = MapReduceState.generateInitializedMapReduceState(
        ds, "Namey", new JobID("14", 28), 12);
    state.setConfigurationXML(
        ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
    state.persist();
    JSONObject result = servlet.handleGetJobDetail(state.getJobID());
    assertJsonEquals(
          "{'mapreduce_id':'job_14_0028',"
        + " 'shards':[],"
        + " 'mapper_spec':{'mapper_params':{}},"
        + " 'name':'Namey',"
        + " 'active':true,"
        + " 'updated_timestamp_ms':-1,"
        + " 'chart_url':'',"
        + " 'configuration':'<?xml version=\\'1.0\\' encoding=\\'UTF-8\\' standalone=\\'no\\'?>"
        +                   "<configuration><\\/configuration>',"
        + " 'counters':{},"
        + " 'start_timestamp_ms':12}", result);
  }
  
  // Tests that a populated job (with a couple of shards) generates a reasonable
  // job detail.
  public void testGetJobDetail_populated() throws Exception {
    MapReduceState state = MapReduceState.generateInitializedMapReduceState(
        ds, "Namey", new JobID("14", 28), 12);
    state.setConfigurationXML(
        ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
    state.persist();
    state.setLastPollTime(24);
    
    // shard1 represents a shard that has lived a full and complete life,
    // with its trusty counter, binky, and a status message left by its
    // loving children.
    ShardState shard1 = ShardState.generateInitializedShardState(ds, 
        new TaskAttemptID(new TaskID(JobID.forName(state.getJobID()), true, 1), 1));
    shard1.setClock(new MockClock(45));
    shard1.setInputSplit(new Configuration(false), new StubInputSplit());
    shard1.setRecordReader(new Configuration(false), new StubRecordReader());
    shard1.setStatusString("Daddy!");
    
    Counters counters1 = new Counters();
    counters1.findCounter("binky", "winky").increment(1);
    shard1.setCounters(counters1);
    
    shard1.setDone();
    shard1.persist();
    
    // shard2 represents the minimal shard that should still work
    ShardState shard2 = ShardState.generateInitializedShardState(ds,
        new TaskAttemptID(new TaskID(JobID.forName(state.getJobID()), true, 2), 1));
    shard2.setClock(new MockClock(77));
    shard2.setInputSplit(new Configuration(false), new StubInputSplit());
    shard2.setRecordReader(new Configuration(false), new StubRecordReader());
    
    shard2.persist();
    
    JSONObject result = servlet.handleGetJobDetail(state.getJobID());
    assertJsonEquals(
          "{'mapreduce_id':'job_14_0028',"
        + " 'shards':[{'shard_description':'Daddy!',"
        + "            'active':false,"
        + "            'updated_timestamp_ms':45,"
        + "            'shard_number':1},"
        + "           {'shard_description':'',"
        + "            'active':true,"
        + "            'updated_timestamp_ms':77,"
        + "            'shard_number':2}],"
        + " 'mapper_spec':{'mapper_params':{}},"
        + " 'name':'Namey',"
        + " 'active':true,"
        + " 'updated_timestamp_ms':-1,"
        + " 'chart_url':'',"
        + " 'configuration':'<?xml version=\\'1.0\\' encoding=\\'UTF-8\\' standalone=\\'no\\'?>"
        +                   "<configuration><\\/configuration>',"
        + " 'counters':{},"
        + " 'start_timestamp_ms':12}", result);
  }
  
  public void testCleanupJob() throws Exception {
    MapReduceState state = MapReduceState.generateInitializedMapReduceState(
        ds, "Namey", new JobID("14", 28), 12);
    state.setConfigurationXML(
        ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
    state.persist();
    JSONObject response = servlet.handleCleanupJob(state.getJobID());
    try {
      MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(state.getJobID()));
      fail("MapReduceState entity should have been removed from the datastore");
    } catch (EntityNotFoundException ignored) {
    }
    assertTrue("Response has \"uccess\" in the status: " + response, 
        ((String) response.get("status")).contains("uccess"));
  }

  public void testCommandError() throws Exception {
    HttpServletRequest request = createMockRequest(
        MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.GET_JOB_DETAIL_PATH, false, true);
    expect(request.getMethod())
      .andReturn("GET")
      .anyTimes();
    HttpServletResponse response = createMock(HttpServletResponse.class);
    PrintWriter responseWriter = createMock(PrintWriter.class);
    responseWriter.print("{\"error_class\":\"java.lang.RuntimeException\","
        + "\"error_message\":\"Full stack trace is available in the server logs. "
        + "Message: blargh\"}");
    responseWriter.flush();
    // This method can't actually throw this exception, but that's not
    // important to the test.
    expect(request.getParameter("mapreduce_id")).andThrow(new RuntimeException("blargh"));
    response.setContentType("application/json");
    expect(response.getWriter()).andReturn(responseWriter).anyTimes();
    replay(request, response, responseWriter);
    servlet.doPost(request, response);
    verify(request, response, responseWriter);
  }
}
