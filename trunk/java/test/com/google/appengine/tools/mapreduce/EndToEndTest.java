// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * An end-to-end test for Hadoop interfaces of mapreduce framework.
 *
 */
public class EndToEndTest extends TestCase {
 private static final Logger logger = Logger.getLogger(EndToEndTest.class.getName());

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true),
          new LocalMemcacheServiceTestConfig());

  MapReduceServlet servlet = new MapReduceServlet();
  private LocalTaskQueue taskQueue;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
    StubMapper.fakeSetUp();
  }

  @Override
  protected void tearDown() throws Exception {
    StubMapper.fakeTearDown();
    helper.tearDown();
    super.tearDown();
  }

  public void testStubInput() throws Exception {
    String queueName = "default";

    Configuration configuration = new Configuration(false);
    configuration.set(AppEngineJobContext.CONTROLLER_QUEUE_KEY, queueName);
    configuration.set(AppEngineJobContext.WORKER_QUEUE_KEY, queueName);
    configuration.set(AppEngineJobContext.MAPPER_SHARD_COUNT_KEY, "2");
    configuration.set(AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY, "1000");
    configuration.setClass("mapreduce.inputformat.class", StubInputFormat.class, InputFormat.class);
    configuration.setClass("mapreduce.map.class", StubMapper.class, AppEngineMapper.class);

    HttpServletRequest request = createMockStartRequest(configuration);
    replay(request);

    AppEngineMapreduce.start(configuration, "test", request);

    executeTasksUntilEmpty(queueName);

    assertTrue(StubMapper.cleanupCalled);
    assertTrue(StubMapper.setupCalled);
    assertTrue(StubMapper.taskCleanupCalled);
    assertTrue(StubMapper.taskSetupCalled);

    Collection<IntWritable> expectedKeys = new HashSet<IntWritable>();
    expectedKeys.addAll(StubInputSplit.KEYS);
    expectedKeys.addAll(StubInputSplit.KEYS);
    assertEquals(expectedKeys, new HashSet<IntWritable>(StubMapper.invocationKeys));
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

  private static HttpServletRequest createMockStartRequest(Configuration configuration) {
    HttpServletRequest request = createMockRequest(MapReduceServlet.START_PATH, false, false);
    expect(request.getParameter(AppEngineJobContext.CONFIGURATION_PARAMETER_NAME))
      .andReturn(ConfigurationXmlUtil.convertConfigurationToXml(configuration))
      .anyTimes();
    return request;
  }

  private void executeTasksUntilEmpty(String queueName) throws Exception {
    while (true) {
      // We have to reacquire task list every time, because local implementation returns a copy.
      List<TaskStateInfo> taskInfo = taskQueue.getQueueStateInfo().get(queueName).getTaskInfo();
      if (taskInfo.isEmpty()) {
        break;
      }
      TaskStateInfo taskStateInfo = taskInfo.get(0);
      taskQueue.deleteTask(queueName, taskStateInfo.getTaskName());
      executeTask(queueName, taskStateInfo);
    }
  }

  private void executeTask(String queueName, TaskStateInfo taskStateInfo) throws Exception {
    logger.info("Executing " + taskStateInfo.getTaskName());

    HttpServletRequest request = createMock(HttpServletRequest.class);
    HttpServletResponse response = createMock(HttpServletResponse.class);

    expect(request.getRequestURI())
        .andReturn(taskStateInfo.getUrl())
        .anyTimes();
    expect(request.getHeader("X-AppEngine-QueueName"))
      .andReturn(queueName)
      .anyTimes();

    Map<String, String> parameters = decodeParameters(taskStateInfo.getBody());
    for (String name : parameters.keySet()) {
      expect(request.getParameter(name))
          .andReturn(parameters.get(name))
          .anyTimes();
    }

    replay(request, response);

    if (taskStateInfo.getMethod().equals("POST")) {
      servlet.doPost(request, response);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  // Sadly there's no way to parse query string with JDK. This is a good enough approximation.
  private static Map<String, String> decodeParameters(String requestBody)
      throws UnsupportedEncodingException {
    Map<String, String> result = new HashMap<String, String>();

    String[] params = requestBody.split("&");
    for (String param : params) {
      String[] pair = param.split("=");
      String name = pair[0];
      String value = URLDecoder.decode(pair[1], "UTF-8");
      if (result.containsKey(name)) {
        throw new IllegalArgumentException("Duplicate parameter: " + requestBody);
      }
      result.put(name, value);
    }

    return result;
  }
}
