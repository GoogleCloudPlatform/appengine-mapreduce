// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 */
public final class MapReduceServletImpl {
// --------------------------- STATIC FIELDS ---------------------------

  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());

  public static final String CONTROLLER_PATH = "controllerCallback";
  public static final String WORKER_PATH = "workerCallback";
  public static final String SHUFFLE_CALLBACK_PATH = "shuffleCallback";
  static final String COMMAND_PATH = "command";

// --------------------------- CONSTRUCTORS ---------------------------

  private MapReduceServletImpl() {
  }

// -------------------------- STATIC METHODS --------------------------

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
    } else if (handler.startsWith(SHUFFLE_CALLBACK_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      handleShuffleCallback(request);
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
      ShardedJobServiceFactory.getShardedJobService().handleControllerRequest(request);
    } else if (handler.startsWith(WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      ShardedJobServiceFactory.getShardedJobService().handleWorkerRequest(request);
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      StatusHandler.handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else if (handler.startsWith(SHUFFLE_CALLBACK_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      handleShuffleCallback(request);
    } else {
      throw new RuntimeException(
          "Received an unknown MapReduce request handler. See logs for more detail.");
    }
  }

  private static void handleShuffleCallback(HttpServletRequest request) {
    String promiseHandle = request.getParameter("promiseHandle");
    String errorCode = request.getParameter("error");
    log.info("shuffle callback; promiseHandle=" + promiseHandle + ", error=" + errorCode);
    try {
      PipelineServiceFactory.newPipelineService().submitPromisedValue(promiseHandle, errorCode);
    } catch (NoSuchObjectException e) {
      // TODO(ohler): retry here rather than letting the task queue retry, to
      // avoid false alarms in the logs.
      throw new RuntimeException("NoSuchObjectException for promiseHandle " + promiseHandle, e);
    } catch (OrphanedObjectException e) {
      // Pipeline is aborted, don't retry.
      log.log(Level.WARNING, "OrphanedObjectException for promiseHandle " + promiseHandle, e);
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
  static void handleStaticResources(String handler, HttpServletResponse response)
      throws IOException {
    String fileName;
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
      fileName = "jquery-1.6.1.min.js";
    } else if (handler.equals("jquery-json.js")) {
      response.setContentType("text/javascript");
      fileName = "jquery.json-2.2.min.js";
    } else if (handler.equals("status.js")) {
      response.setContentType("text/javascript");
      fileName = "status.js";
    } else {
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    response.setHeader("Cache-Control", "public; max-age=300");

    try {
      InputStream resourceStream = MapReduceServlet.class.getResourceAsStream(
          "/com/google/appengine/tools/mapreduce/" + fileName);
      if (resourceStream == null) {
        resourceStream = MapReduceServlet.class.getResourceAsStream(
            "/third_party/java_src/appengine_mapreduce2/static/" + fileName);
      }
      if (resourceStream == null) {
        throw new RuntimeException("Couldn't find static file for MapReduce library: " + fileName);
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
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Couldn't find static file for MapReduce library", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't read static file for MapReduce library", e);
    }
  }
}
