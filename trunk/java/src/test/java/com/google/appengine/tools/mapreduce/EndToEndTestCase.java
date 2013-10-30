// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import com.google.appengine.api.blobstore.dev.LocalBlobstoreService;
import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.HeaderWrapper;
import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.appengine.tools.pipeline.impl.servlets.TaskHandler;
import com.google.apphosting.api.ApiProxy;
import com.google.common.base.CharMatcher;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 */
public abstract class EndToEndTestCase extends TestCase {
// --------------------------- STATIC FIELDS ---------------------------

  private static final Logger logger = Logger.getLogger(EndToEndTestCase.class.getName());

// ------------------------------ FIELDS ------------------------------

  private final MapReduceServlet mrServlet = new MapReduceServlet();
  private final PipelineServlet pipelineServlet = new PipelineServlet();
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig().setDisableAutoTaskExecution(true),
          new LocalMemcacheServiceTestConfig(),
          new LocalFileServiceTestConfig());
  private LocalTaskQueue taskQueue;

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    taskQueue = LocalTaskQueueTestConfig.getLocalTaskQueue();
    ApiProxyLocal proxy = (ApiProxyLocal) ApiProxy.getDelegate();
    // Creating files is not allowed in some test execution environments, so don't.
    proxy.setProperty(LocalBlobstoreService.NO_STORAGE_PROPERTY, "true");
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

// -------------------------- INSTANCE METHODS --------------------------

  private void executeTask(String queueName, QueueStateInfo.TaskStateInfo taskStateInfo)
      throws Exception {
    logger.info("Executing task " + taskStateInfo.getTaskName()
        + " with URL " + taskStateInfo.getUrl());
    // Hack to allow for deferred tasks. Exploits knowing how they work.
    if (taskStateInfo.getUrl().endsWith("__deferred__")) {
      ObjectInputStream oin =
          new ObjectInputStream(new ByteArrayInputStream(taskStateInfo.getBodyAsBytes()));
      Runnable object = (Runnable) oin.readObject();
      object.run();
      return;
    }
    HttpServletRequest request = createMock(HttpServletRequest.class);
    HttpServletResponse response = createMock(HttpServletResponse.class);

    String pathInfo = taskStateInfo.getUrl();
    if (pathInfo.startsWith("/")) {
      int skipFrom = pathInfo.startsWith("/_ah/") ? 5 : 1;
      pathInfo = pathInfo.substring(pathInfo.indexOf('/', skipFrom));
    } else {
      pathInfo = "/" + pathInfo;
    }
    expect(request.getPathInfo()).andReturn(pathInfo).anyTimes();
    expect(request.getHeader("X-AppEngine-QueueName")).andReturn(queueName).anyTimes();
    expect(request.getHeader("X-AppEngine-TaskName")).andReturn(taskStateInfo.getTaskName())
        .anyTimes();
    // Pipeline looks at this header but uses the value only for diagnostic messages
    expect(request.getHeader(TaskHandler.TASK_RETRY_COUNT_HEADER))
        .andReturn("HACK: not implemented").anyTimes();
    for (HeaderWrapper header : taskStateInfo.getHeaders()) {
      int value = parseAsQuotedInt(header.getValue());
      expect(request.getIntHeader(header.getKey())).andReturn(value).anyTimes();
      logger.info("header: " + header.getKey() + "=" + header.getValue());
      expect(request.getHeader(header.getKey())).andReturn(header.getValue()).anyTimes();
    }

    Map<String, String> parameters = decodeParameters(taskStateInfo.getBody());
    for (String name : parameters.keySet()) {
      expect(request.getParameter(name)).andReturn(parameters.get(name)).anyTimes();
    }
    expect(request.getParameterNames()).andReturn(Collections.enumeration(parameters.keySet()))
        .anyTimes();

    replay(request, response);

    if (taskStateInfo.getMethod().equals("POST")) {
      if (taskStateInfo.getUrl().startsWith(PipelineServlet.BASE_URL)) {
        pipelineServlet.doPost(request, response);
      } else {
        mrServlet.doPost(request, response);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private int parseAsQuotedInt(String str) {
    try {
      return Integer.parseInt(CharMatcher.is('"').trimFrom(str));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  protected void executeTasksUntilEmpty() throws Exception {
    executeTasksUntilEmpty("default");
  }

  protected void executeTasksUntilEmpty(String queueName) throws Exception {
    while (true) {
      // We have to re-acquire task list every time, because local implementation returns a copy.
      List<QueueStateInfo.TaskStateInfo> taskInfo =
          taskQueue.getQueueStateInfo().get(queueName).getTaskInfo();
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
