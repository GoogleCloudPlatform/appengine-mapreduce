// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertFalse;

import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;

import org.junit.After;
import org.junit.Before;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 */
public abstract class EndToEndTestCase {

  private static final Logger logger = Logger.getLogger(EndToEndTestCase.class.getName());

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true),
          new LocalModulesServiceTestConfig());
  protected LocalTaskQueue taskQueue;

  protected final ShardedJobService service = ShardedJobServiceFactory.getShardedJobService();

  protected final String controllerPath = "controller";
  protected final String workerPath = "worker";
  protected ShardedJobSettings settings;

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
    settings = new ShardedJobSettings.Builder()
        .setControllerPath("/controller")
        .setWorkerPath("/worker")
        .build();
  }

  @After
  public void tearDown() throws Exception {
    helper.tearDown();
  }

  protected void executeTask(String queueName, QueueStateInfo.TaskStateInfo taskStateInfo)
      throws Exception {
    logger.info("Executing " + taskStateInfo.getTaskName());

    HttpServletRequest request = createMock(HttpServletRequest.class);

    expect(request.getRequestURI())
        .andReturn(taskStateInfo.getUrl())
        .anyTimes();
    expect(request.getHeader("X-AppEngine-QueueName"))
        .andReturn(queueName)
        .anyTimes();

    Map<String, String> parameters = decodeParameters(taskStateInfo);
    for (String name : parameters.keySet()) {
      expect(request.getParameter(name))
          .andReturn(parameters.get(name))
          .anyTimes();
    }

    replay(request);

    if (taskStateInfo.getMethod().equals("POST")) {
      if (taskStateInfo.getUrl().contains(controllerPath)) {
        new ShardedJobRunner<>().completeShard(
            checkNotNull(request.getParameter(ShardedJobHandler.JOB_ID_PARAM), "Null job id"),
            checkNotNull(request.getParameter(ShardedJobHandler.TASK_ID_PARAM), "Null task id"));
      } else {
        new ShardedJobRunner<>().runTask(
            checkNotNull(request.getParameter(ShardedJobHandler.JOB_ID_PARAM), "Null job id"),
            checkNotNull(request.getParameter(ShardedJobHandler.TASK_ID_PARAM), "Null task id"),
            Integer.parseInt(request.getParameter(ShardedJobHandler.SEQUENCE_NUMBER_PARAM)));
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

  protected String getTaskId(TaskStateInfo taskStateInfo) throws UnsupportedEncodingException {
    return decodeParameters(taskStateInfo).get(ShardedJobHandler.TASK_ID_PARAM);
  }

  // Sadly there's no way to parse query string with JDK. This is a good enough approximation.
  private static Map<String, String> decodeParameters(TaskStateInfo taskStateInfo)
      throws UnsupportedEncodingException {
    Map<String, String> result = new HashMap<>();

    String[] params = taskStateInfo.getBody().split("&");
    for (String param : params) {
      String[] pair = param.split("=");
      String name = pair[0];
      String value = URLDecoder.decode(pair[1], "UTF-8");
      if (result.containsKey(name)) {
        throw new IllegalArgumentException("Duplicate parameter: " + taskStateInfo.getBody());
      }
      result.put(name, value);
    }

    return result;
  }

  protected TaskStateInfo grabNextTaskFromQueue(String queueName) {
    List<TaskStateInfo> taskInfo = getTasks(queueName);
    assertFalse(taskInfo.isEmpty());
    TaskStateInfo taskStateInfo = taskInfo.get(0);
    taskQueue.deleteTask(queueName, taskStateInfo.getTaskName());
    return taskStateInfo;
  }
}
