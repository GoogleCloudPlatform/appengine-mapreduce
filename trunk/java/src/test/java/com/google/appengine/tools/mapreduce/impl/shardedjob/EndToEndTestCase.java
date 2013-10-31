// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import junit.framework.TestCase;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 */
public abstract class EndToEndTestCase extends TestCase {
// --------------------------- STATIC FIELDS ---------------------------

  private static final Logger logger = Logger.getLogger(EndToEndTestCase.class.getName());

// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true));
  private LocalTaskQueue taskQueue;

  protected final ShardedJobService service = ShardedJobServiceFactory.getShardedJobService();

  protected final String controllerPath = "controller";
  protected final String workerPath = "worker";
  protected ShardedJobSettings settings;

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
    settings = new ShardedJobSettings()
        .setControllerPath("/controller")
        .setWorkerPath("/worker");
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void executeTask(String queueName, QueueStateInfo.TaskStateInfo taskStateInfo)
      throws Exception {
    logger.info("Executing " + taskStateInfo.getTaskName());

    HttpServletRequest request = createMock(HttpServletRequest.class);

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

    replay(request);

    if (taskStateInfo.getMethod().equals("POST")) {
      if (taskStateInfo.getUrl().contains(controllerPath)) {
        service.handleShardCompleteRequest(request);
      } else {
        service.handleWorkerRequest(request);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  protected List<QueueStateInfo.TaskStateInfo> getTasks() {
    return getTasks("default");
  }

  protected List<QueueStateInfo.TaskStateInfo> getTasks(String queueName) {
    return taskQueue.getQueueStateInfo().get(queueName).getTaskInfo();
  }

  protected void executeTasksUntilEmpty() throws Exception {
    executeTasksUntilEmpty("default");
  }

  protected void executeTasksUntilEmpty(String queueName) throws Exception {
    while (true) {
      // We have to reacquire task list every time, because local implementation returns a copy.
      List<QueueStateInfo.TaskStateInfo> taskInfo = getTasks(queueName);
      if (taskInfo.isEmpty()) {
        break;
      }
      QueueStateInfo.TaskStateInfo taskStateInfo = taskInfo.get(0);
      taskQueue.deleteTask(queueName, taskStateInfo.getTaskName());
      executeTask(queueName, taskStateInfo);
    }
  }

// -------------------------- STATIC METHODS --------------------------

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
