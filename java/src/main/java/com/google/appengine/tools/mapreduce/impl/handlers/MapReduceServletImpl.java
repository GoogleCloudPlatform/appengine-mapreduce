// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler.JOB_ID_PARAM;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler.SEQUENCE_NUMBER_PARAM;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobHandler.TASK_ID_PARAM;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobRunner;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 */
public final class MapReduceServletImpl {

  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());
  private static final Map<String, Resource> RESOURCES = ImmutableMap.<String, Resource>builder()
      .put("status", new Resource("/_ah/pipeline/list?class_path=" + MapReduceJob.class.getName()))
      .put("detail", new Resource("detail.html", "text/html"))
      .put("base.css", new Resource("base.css", "text/css"))
      .put("jquery.js", new Resource("jquery-1.6.1.min.js", "text/javascript"))
      .put("jquery-json.js", new Resource("jquery.json-2.2.min.js", "text/javascript"))
      .put("jquery-url.js", new Resource("jquery.url.js", "text/javascript"))
      .put("status.js", new Resource("status.js", "text/javascript"))
      .build();

  static final String COMMAND_PATH = "command";

  private static class Resource {
    private final String filename;
    private final String contentType;
    private final String redirect;

    Resource(String filename, String contentType) {
      this.filename = filename;
      this.contentType = contentType;
      this.redirect = null;
    }

    Resource(String redirect) {
      this.redirect = redirect;
      filename = null;
      contentType = null;
    }

    String getRedirect() {
      return redirect;
    }

    String getFilename() {
      return filename;
    }

    String getContentType() {
      return contentType;
    }
  }

  private MapReduceServletImpl() {
  }

  /**
   * Handle GET http requests.
   */
  public static void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String handler = getHandler(request);
    if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      StatusHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      handleStaticResources(handler, response);
    }
  }

  /**
   * Handle POST http requests.
   */
  public static void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String handler = getHandler(request);
    if (handler.startsWith(CONTROLLER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      new ShardedJobRunner<>().completeShard(
          checkNotNull(request.getParameter(JOB_ID_PARAM), "Null job id"),
          checkNotNull(request.getParameter(TASK_ID_PARAM), "Null task id"));
    } else if (handler.startsWith(WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      new ShardedJobRunner<>().runTask(
          checkNotNull(request.getParameter(JOB_ID_PARAM), "Null job id"),
          checkNotNull(request.getParameter(TASK_ID_PARAM), "Null task id"),
          Integer.parseInt(request.getParameter(SEQUENCE_NUMBER_PARAM)));
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      StatusHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      throw new RuntimeException(
          "Received an unknown MapReduce request handler. See logs for more detail.");
    }
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
  private static boolean checkForAjax(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!"XMLHttpRequest".equals(request.getHeader("X-Requested-With"))) {
      log.log(
          Level.SEVERE, "Received unexpected non-XMLHttpRequest command. Possible CSRF attack.");
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Received unexpected non-XMLHttpRequest command.");
      return false;
    }
    return true;
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
  private static boolean checkForTaskQueue(HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    if (request.getHeader("X-AppEngine-QueueName") == null) {
      log.log(Level.SEVERE, "Received unexpected non-task queue request. Possible CSRF attack.");
      response.sendError(
          HttpServletResponse.SC_FORBIDDEN, "Received unexpected non-task queue request.");
      return false;
    }
    return true;
  }

  /**
   * Returns the handler portion of the URL path.
   *
   * For examples (for a servlet mapped as /foo/*):
   *   getHandler(https://www.google.com/foo/bar) -> bar
   *   getHandler(https://www.google.com/foo/bar/id) -> bar/id
   */
  private static String getHandler(HttpServletRequest request) {
    String pathInfo = request.getPathInfo();
    return pathInfo == null ? "" : pathInfo.substring(1);
  }

  /**
   * Handle serving of static resources (which we do dynamically so users
   * only have to add one entry to their web.xml).
   */
  @SuppressWarnings("resource")
  static void handleStaticResources(String handler, HttpServletResponse response)
      throws IOException {
    Resource resource = RESOURCES.get(handler);
    if (resource == null) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    if (resource.getRedirect() != null) {
      response.sendRedirect(resource.getRedirect());
      return;
    }
    response.setContentType(resource.getContentType());
    response.setHeader("Cache-Control", "public; max-age=300");
    try {
      InputStream resourceStream = MapReduceServlet.class.getResourceAsStream(
          "/com/google/appengine/tools/mapreduce/" + resource.getFilename());
      if (resourceStream == null) {
        resourceStream = MapReduceServlet.class.getResourceAsStream(
            "/third_party/java_src/appengine_mapreduce2/static/" + resource.getFilename());
      }
      if (resourceStream == null) {
        throw new RuntimeException("Missing MapReduce static file " + resource.getFilename());
      }
      OutputStream responseStream = response.getOutputStream();
      byte[] buffer = new byte[1024];
      while (true) {
        int bytesRead = resourceStream.read(buffer);
        if (bytesRead < 0) {
          break;
        }
        responseStream.write(buffer, 0, bytesRead);
      }
      responseStream.flush();
    } catch (IOException e) {
      throw new RuntimeException("Couldn't read static file for MapReduce library", e);
    }
  }
}
