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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.easymock.EasyMock;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests MapReduceServlet
 *
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

  private static HttpServletRequest createMockStartJobRequest(Configuration conf) {
    HttpServletRequest request = createMockRequest(
        MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.START_JOB_PATH, false, true);
    expect(request.getMethod())
      .andReturn("POST")
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
   * Compares a string representation of the expected JSON object
   * with the actual, ignoring white space and converting single quotes
   * to double quotes.
   */
  private static void assertJsonEquals(String expected, JSONObject actual) {
    assertEquals(expected.replace('\'', '"').replace(" ", ""),
        actual.toString().replace(" ", "").replace("\\r\\n", "").replace("\\n", ""));
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

  public void testCommandError() throws Exception {
    HttpServletRequest request = createMockRequest(
        MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.GET_JOB_DETAIL_PATH, false, true);
    expect(request.getMethod())
      .andReturn("GET")
      .anyTimes();
    HttpServletResponse response = createMock(HttpServletResponse.class);
    PrintWriter responseWriter = createMock(PrintWriter.class);
    responseWriter.write('{');
    responseWriter.write("\"error_class\"");
    responseWriter.write(':');
    responseWriter.write("\"java.lang.RuntimeException\"");
    responseWriter.write(',');
    responseWriter.write("\"error_message\"");
    responseWriter.write(':');
    responseWriter.write("\"Full stack trace is available in the server logs. "
        + "Message: blargh\"");
    responseWriter.write('}');
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
