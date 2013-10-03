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

package com.google.appengine.tools.mapreduce.impl.handlers;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.common.collect.ImmutableList;

import junit.framework.TestCase;

import org.easymock.EasyMock;

import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests MapReduceServlet
 *
 */
public class MapReduceServletTest extends TestCase {
// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

  private MapReduceServlet servlet;

// ------------------------ OVERRIDING METHODS ------------------------

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    servlet = new MapReduceServlet();
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

// -------------------------- TEST METHODS --------------------------

  public void testBailsOnBadHandler() throws Exception {
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

  public void testCommandError() throws Exception {
    HttpServletRequest request = createMockRequest(
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, false, true);
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

  public void testControllerCSRF() throws Exception {
    String jobId = "testJob";
    // Send it as an AJAX request but not a task queue request - should be denied.
    HttpServletRequest request = createMockRequest(MapReduceServletImpl.CONTROLLER_PATH,
        false, true);
    HttpServletResponse response = createMock(HttpServletResponse.class);
    response.sendError(403, "Received unexpected non-task queue request.");
    replay(request, response);
    servlet.doPost(request, response);
    verify(request, response);
  }

  public void testGetCommand() {
    HttpServletRequest request = createMockRequest(
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.ABORT_JOB_PATH, false, true);
    replay(request);

    // Command should be treated as part of the handler
    assertEquals("command/abort_job", MapReduceServletImpl.getHandler(request));
    verify(request);
  }

  public void testGetHandler() {
    List<String> prefixes = ImmutableList.of("", "map/", "map/jobid1/");
    for (String prefix : prefixes) {
      HttpServletRequest request =
          createMockControllerRequest(prefix + MapReduceServletImpl.CONTROLLER_PATH);
      replay(request);
      assertEquals("controllerCallback", MapReduceServletImpl.getHandler(request));
      verify(request);
    }
  }

  public void testGetJobDetailCSRF() throws Exception {
    String jobId = "testJob";
    // Send it as a task queue request but not an ajax request - should be denied.
    HttpServletRequest request = createMockRequest(
        MapReduceServletImpl.COMMAND_PATH + "/" + StatusHandler.GET_JOB_DETAIL_PATH, true, false);
    expect(request.getMethod())
        .andReturn("POST")
        .anyTimes();

    HttpServletResponse response = createMock(HttpServletResponse.class);

    // Set before error and last one wins, so this is harmless.
    response.setContentType("application/json");
    EasyMock.expectLastCall().anyTimes();

    response.sendError(403, "Received unexpected non-XMLHttpRequest command.");
    replay(request, response);
    servlet.doGet(request, response);
    verify(request, response);
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
    MapReduceServletImpl.handleStaticResources("jquery.js", resp);
    verify(resp, sos);
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
    MapReduceServletImpl.handleStaticResources("status", resp);
    verify(resp, sos);
  }

// -------------------------- STATIC METHODS --------------------------

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

  private static HttpServletRequest createMockControllerRequest(String handlerPath) {
    HttpServletRequest request = createMockRequest(handlerPath, true, false);
    return request;
  }
}
