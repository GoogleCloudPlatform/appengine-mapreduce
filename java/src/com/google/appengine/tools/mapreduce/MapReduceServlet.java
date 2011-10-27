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

import com.google.appengine.tools.mapreduce.v2.impl.handlers.Controller;
import com.google.appengine.tools.mapreduce.v2.impl.handlers.Status;
import com.google.appengine.tools.mapreduce.v2.impl.handlers.Worker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet for all MapReduce API related functions.
 *
 * This should be specified as the handler for MapReduce URLs in web.xml.
 * For instance:
 * <pre>
 * {@code
 * <servlet>
 *   <servlet-name>mapreduce</servlet-name>
 *   <servlet-class>com.google.appengine.tools.mapreduce.MapReduceServlet</servlet-class>
 * </servlet>
 * <servlet-mapping>
 *   <servlet-name>mapreduce</servlet-name>
 *   <url-pattern>/mapreduce/*</url-pattern>
 * </servlet-mapping>
 * }
 *
 * Generally you'll want this handler to be protected by an admin security constraint
 * (see <a
 * href="http://code.google.com/appengine/docs/java/config/webxml.html#Security_and_Authentication">
 * Security and Authentication</a>)
 * for more details.
 * </pre>
 *
 */
public class MapReduceServlet extends HttpServlet {
  /**
   *
   */
  public static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;

  public static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());

  // Amount of time to spend on actual map() calls per task execution.
  public static final int PROCESSING_TIME_PER_TASK_MS = 10000;

  /**
   * Default amount of quota to divvy up per controller execution.
   */
  public static final int DEFAULT_QUOTA_BATCH_SIZE = 20;

  // VisibleForTesting
  public static final String CONTROLLER_PATH = "controllerCallback";
  public static final String START_PATH = "start";
  public static final String MAPPER_WORKER_PATH = "mapperCallback";
  public static final String COMMAND_PATH = "command";

  // Command paths
  public static final String LIST_JOBS_PATH = "list_jobs";
  public static final String LIST_CONFIGS_PATH = "list_configs";
  public static final String CLEANUP_JOB_PATH = "cleanup_job";
  public static final String ABORT_JOB_PATH = "abort_job";
  public static final String GET_JOB_DETAIL_PATH = "get_job_detail";
  public static final String START_JOB_PATH = "start_job";


  /**
   * Returns the portion of the URL from the end of the TLD (exclusive) to the
   * handler portion (exclusive).
   *
   * For example, getBase(https://www.google.com/foo/bar) -> /foo/
   * However, there are handler portions that take more than segment
   * (currently only the command handlers). So in that case, we have:
   * getBase(https://www.google.com/foo/command/bar) -> /foo/
   */
  public static String getBase(HttpServletRequest request) {
    String fullPath = request.getRequestURI();
    int baseEnd = getDividingIndex(fullPath);
    return fullPath.substring(0, baseEnd + 1);
  }

  /**
   * Finds the index of the "/" separating the base from the handler.
   */
  private static int getDividingIndex(String fullPath) {
    int baseEnd = fullPath.lastIndexOf("/");
    if (fullPath.substring(0, baseEnd).endsWith(COMMAND_PATH)) {
      baseEnd = fullPath.substring(0, baseEnd).lastIndexOf("/");
    }
    return baseEnd;
  }

  /**
   * Returns the handler portion of the URL path.
   *
   * For example, getHandler(https://www.google.com/foo/bar) -> bar
   * Note that for command handlers,
   * getHandler(https://www.google.com/foo/command/bar) -> command/bar
   */
  static String getHandler(HttpServletRequest request) {
    String requestURI = request.getRequestURI();
    return requestURI.substring(getDividingIndex(requestURI) + 1);
  }

  /**
   * Checks to ensure that the current request was sent via the task queue.
   *
   * If the request is not in the task queue, returns false, and sets the
   * response status code to 403. This protects against CSRF attacks against
   * task queue-only handlers.
   *
   * @return true if the request is a task queue request
   */
  private static boolean checkForTaskQueue(HttpServletRequest request, HttpServletResponse response) {
    if (request.getHeader("X-AppEngine-QueueName") == null) {
      log.log(Level.SEVERE, "Received unexpected non-task queue request. Possible CSRF attack.");
      try {
        response.sendError(
            HttpServletResponse.SC_FORBIDDEN, "Received unexpected non-task queue request.");
      } catch (IOException ioe) {
        throw new RuntimeException("Encountered error writing error", ioe);
      }
      return false;
    }
    return true;
  }

  /**
   * Checks to ensure that the current request was sent via an AJAX request.
   *
   * If the request was not sent by an AJAX request, returns false, and sets
   * the response status code to 403. This protects against CSRF attacks against
   * AJAX only handlers.
   *
   * @return true if the request is a task queue request
   */
  private static boolean checkForAjax(HttpServletRequest request, HttpServletResponse response) {
    if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
      log.log(
          Level.SEVERE, "Received unexpected non-XMLHttpRequest command. Possible CSRF attack.");
      try {
        response.sendError(HttpServletResponse.SC_FORBIDDEN,
            "Received unexpected non-XMLHttpRequest command.");
      } catch (IOException ioe) {
        throw new RuntimeException("Encountered error writing error", ioe);
      }
      return false;
    }
    return true;
  }

  /**
   * Handles all MapReduce callbacks.
   */
  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) {
    String handler = MapReduceServlet.getHandler(request);
    if (handler.startsWith(CONTROLLER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      Controller.handleController(request);
    } else if (handler.startsWith(MAPPER_WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      Worker.handleMapperWorker(request);
    } else if (handler.startsWith(START_PATH)) {
      // We don't add a GET handler for this one, since we're expecting the user
      // to POST the whole XML specification.
      // TODO(user): Make name customizable.
      // TODO(user): Add ability to specify a redirect.
      Controller.handleStart(
          ConfigurationXmlUtil.getConfigurationFromXml(request.getParameter("configuration")),
          "Automatically run request", request);
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      Status.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      throw new RuntimeException(
          "Received an unknown MapReduce request handler. See logs for more detail.");
    }
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    String handler = MapReduceServlet.getHandler(request);
    if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      Status.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      handleStaticResources(handler, response);
    }
  }

  /**
   * Handle serving of static resources (which we do dynamically so users
   * only have to add one entry to their web.xml).
   */
  public static void handleStaticResources(String handler, HttpServletResponse response) {
    String fileName = null;
    if (handler.equals("status")) {
      response.setContentType("text/html");
      fileName = "overview.html";
    } else if (handler.equals("detail")) {
      response.setContentType("text/html");
      fileName = "detail.html";
    } else if (handler.equals("base.css")) {
      response.setContentType("text/css");
      fileName = "base.css";
    } else if (handler.equals("jquery.js")) {
      response.setContentType("text/javascript");
      fileName = "jquery-1.4.2.min.js";
    } else if (handler.equals("status.js")) {
      response.setContentType("text/javascript");
      fileName = "status.js";
    } else {
      try {
        response.sendError(404);
      } catch (IOException e) {
        throw new RuntimeException("Encountered error sending 404", e);
      }
      return;
    }

    response.setHeader("Cache-Control", "public; max-age=300");

    try {
      InputStream resourceStream = MapReduceServlet.class.getResourceAsStream(
          "/com/google/appengine/tools/mapreduce/" + fileName);
      if (resourceStream == null) {
        resourceStream = MapReduceServlet.class.getResourceAsStream(
          "/third_party/java_src/appengine_mapreduce/" + fileName);
      }
      if (resourceStream == null) {
        throw new RuntimeException("Couldn't find static file for MapReduce library: " + fileName);
      }
      OutputStream responseStream = response.getOutputStream();
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = resourceStream.read(buffer)) != -1) {
        responseStream.write(buffer, 0, bytesRead);
      }
      responseStream.flush();
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Couldn't find static file for MapReduce library", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't read static file for MapReduce library", e);
    }
  }
}
