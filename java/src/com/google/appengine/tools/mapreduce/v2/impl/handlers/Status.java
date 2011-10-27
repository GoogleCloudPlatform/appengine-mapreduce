// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.mapreduce.ConfigurationTemplatePreprocessor;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.MapReduceXml;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * UI Status view logic handler.
 *
 */
public class Status {
  private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

  /**
   * Handle the get_job_detail AJAX command.
   */
  public static JSONObject handleGetJobDetail(String jobId) {
    MapReduceState state;
    try {
      state = MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId));
    } catch (EntityNotFoundException e) {
      throw new IllegalArgumentException("Couldn't find MapReduce for id:" + jobId, e);
    }
    return state.toJson(true);
  }

  /**
   * Handle the cleanup_job AJAX command.
   */
  public static JSONObject handleCleanupJob(String jobId) {
    JSONObject retValue = new JSONObject();
    try {
      try {
        MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId)).delete();
        retValue.put("status", "Successfully deleted requested job.");
      } catch (IllegalArgumentException e) {
        retValue.put("status", "Couldn't find requested job.");
      } catch (EntityNotFoundException e) {
        retValue.put("status", "Couldn't find requested job.");
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }
    return retValue;
  }

  /**
   * Handle the start_job AJAX command.
   */
  public static JSONObject handleStartJob(Map<String, String> params, String name,
      HttpServletRequest request) {
    try {
      MapReduceXml mrXml = MapReduceXml.getMapReduceXmlFromFile();
      Configuration configuration = mrXml.instantiateConfiguration(name, params);
      // TODO(user): What should we be doing here for error handling?
      String jobId = Controller.handleStart(configuration, name, request);
      JSONObject retValue = new JSONObject();
      try {
        retValue.put("mapreduce_id", jobId);
      } catch (JSONException e) {
        throw new RuntimeException("Hard-coded string is null");
      }
      return retValue;
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Couldn't find mapreduce.xml", e);
    }
  }

  /**
   * Handle the list_jobs AJAX command.
   */
  public static JSONObject handleListJobs(String cursor, int count) {
    List<MapReduceState> states = new ArrayList<MapReduceState>();
    Cursor newCursor = MapReduceState.getMapReduceStates(ds, cursor, count, states);
    JSONArray jobs = new JSONArray();
    for (MapReduceState state : states) {
      jobs.put(state.toJson(false));
    }

    JSONObject retValue = new JSONObject();
    try {
      retValue.put("jobs", jobs);
      if (newCursor != null) {
        retValue.put("cursor", newCursor.toWebSafeString());
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return retValue;
  }

  /**
   * Handle the list_configs AJAX command.
   */
  public static JSONObject handleListConfigs(MapReduceXml xml) {
    JSONObject retValue = new JSONObject();
    JSONArray configArray = new JSONArray();
    Set<String> names = xml.getConfigurationNames();
    for (String name : names) {
      String configXml = xml.getTemplateAsXmlString(name);
      ConfigurationTemplatePreprocessor preprocessor =
        new ConfigurationTemplatePreprocessor(configXml);
      configArray.put(preprocessor.toJson(name));
    }
    try {
      retValue.put("configs", configArray);
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null");
    }
    return retValue;
  }

  /**
   * Handles all status page commands.
   */
  public static void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    JSONObject retValue = null;
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    try {
      if (command.equals(MapReduceServlet.LIST_CONFIGS_PATH) && !isPost) {
        MapReduceXml xml;
        try {
          xml = MapReduceXml.getMapReduceXmlFromFile();
          retValue = handleListConfigs(xml);
        } catch (FileNotFoundException e) {
          retValue = new JSONObject();
          retValue.put("status", "Couldn't find mapreduce.xml file");
        }
      } else if (command.equals(MapReduceServlet.LIST_JOBS_PATH) && !isPost) {
        String cursor = request.getParameter("cursor");
        String countString = request.getParameter("count");
        int count = MapReduceServlet.DEFAULT_JOBS_PER_PAGE_COUNT;
        if (countString != null) {
          count = Integer.parseInt(countString);
        }

        retValue = handleListJobs(cursor, count);
      } else if (command.equals(MapReduceServlet.CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(MapReduceServlet.ABORT_JOB_PATH) && isPost) {
        retValue = Controller.handleAbortJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(MapReduceServlet.GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(request.getParameter("mapreduce_id"));
      } else if (command.equals(MapReduceServlet.START_JOB_PATH) && isPost) {
        Map<String, String> templateParams = new TreeMap<String, String>();
        Map httpParams = request.getParameterMap();
        for (Object paramObject : httpParams.keySet()) {
          String param = (String) paramObject;
          if (param.startsWith("mapper_params.")) {
            templateParams.put(param.substring("mapper_params.".length()),
                ((String[]) httpParams.get(param))[0]);
          }
        }
        retValue = handleStartJob(templateParams, ((String []) httpParams.get("name"))[0], request);
      } else {
        response.sendError(404);
        return;
      }
    } catch (Throwable t) {
      MapReduceServlet.log.log(Level.SEVERE, "Got exception while running command", t);
      try {
        retValue = new JSONObject();
        retValue.put("error_class", t.getClass().getName());
        retValue.put("error_message",
            "Full stack trace is available in the server logs. Message: "
            + t.getMessage());
      } catch (JSONException e) {
        throw new RuntimeException("Couldn't create error JSON object", e);
      }
    }
    try {
      retValue.write(response.getWriter());
      response.getWriter().flush();
    } catch (JSONException e) {
        throw new RuntimeException("Couldn't write command response", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't write command response", e);
    }
  }

}
