// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.Counter;
import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.impl.WorkerResult;
import com.google.appengine.tools.mapreduce.impl.WorkerShardState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.googlecode.charts4j.AxisLabelsFactory;
import com.googlecode.charts4j.BarChart;
import com.googlecode.charts4j.Data;
import com.googlecode.charts4j.DataUtil;
import com.googlecode.charts4j.GCharts;
import com.googlecode.charts4j.Plot;
import com.googlecode.charts4j.Plots;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * UI Status view logic handler.
 *
 */
final class StatusHandler {
  private static final Logger log = Logger.getLogger(StatusHandler.class.getName());

  public static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;
  // Command paths
  public static final String LIST_JOBS_PATH = "list_jobs";
  public static final String CLEANUP_JOB_PATH = "cleanup_job";
  public static final String ABORT_JOB_PATH = "abort_job";
  public static final String GET_JOB_DETAIL_PATH = "get_job_detail";

  // --------------------------- CONSTRUCTORS ---------------------------

  private StatusHandler() {
  }

  // -------------------------- STATIC METHODS --------------------------

  private static JSONObject handleCleanupJob(String jobId) throws JSONException {
    JSONObject retValue = new JSONObject();
    ShardedJobServiceFactory.getShardedJobService().cleanupJob(jobId);
    retValue.put("status", "Successfully deleted requested job.");
    return retValue;
  }

  private static JSONObject handleAbortJob(String jobId) throws JSONException {
    JSONObject retValue = new JSONObject();
    ShardedJobServiceFactory.getShardedJobService().abortJob(jobId);
    retValue.put("status", "Successfully aborted requested job.");
    return retValue;
  }

  /**
   * Handles all status page commands.
   */
  static void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    JSONObject retValue;
    try {
      if (command.equals(LIST_JOBS_PATH) && !isPost) {
        retValue = handleListJobs(request);
      } else if (command.equals(CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(ABORT_JOB_PATH) && isPost) {
        retValue = handleAbortJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(request.getParameter("mapreduce_id"));
      } else {
        response.sendError(HttpServletResponse.SC_NOT_FOUND);
        return;
      }
    } catch (Exception t) {
      log.log(Level.SEVERE, "Got exception while running command", t);
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

  private static JSONObject toJson(Counters counters) throws JSONException {
    JSONObject retValue = new JSONObject();
    for (Counter counter : counters.getCounters()) {
      retValue.put(counter.getName(), counter.getValue());
    }
    return retValue;
  }

  private static String getChartUrl(int shardCount,
      Map<Integer, WorkerShardState> workerShardStates) {
    List<Long> processedCounts = Lists.newArrayListWithCapacity(workerShardStates.size());
    for (int i = 0; i < shardCount; i++) {
      processedCounts.add(workerShardStates.get(i) == null ? 0
          : workerShardStates.get(i).getWorkerCallCount());
    }
    // If max == 0, the numeric range will be from 0 to 0. This causes some
    // problems when scaling to the range, so add 1 to max, assuming that the
    // smallest value can be 0, and this ensures that the chart always shows,
    // at a minimum, a range from 0 to 1 - when all shards are just starting.
    long maxPlusOne = processedCounts.isEmpty() ? 1 : Collections.max(processedCounts) + 1;

    List<String> countLabels = new ArrayList<String>();
    for (int i = 0; i < processedCounts.size(); i++) {
      countLabels.add(String.valueOf(i));
    }

    Data countData = DataUtil.scaleWithinRange(0, maxPlusOne, processedCounts);

    // TODO(user): Rather than returning charts from both servers, let's just
    // do it on the client's end.
    Plot countPlot = Plots.newBarChartPlot(countData);
    BarChart countChart = GCharts.newBarChart(countPlot);
    countChart.addYAxisLabels(AxisLabelsFactory.newNumericRangeAxisLabels(0, maxPlusOne));
    countChart.addXAxisLabels(AxisLabelsFactory.newAxisLabels(countLabels));
    countChart.setSize(300, 200);
    countChart.setBarWidth(BarChart.AUTO_RESIZE);
    countChart.setSpaceBetweenGroupsOfBars(1);
    return countChart.toURLString();
  }

  /**
   * Handle the get_job_detail AJAX command.
   */
  @VisibleForTesting
  static JSONObject handleGetJobDetail(String jobId) {
    ShardedJobState<?, WorkerResult<?>> state =
        ShardedJobServiceFactory.getShardedJobService().getJobState(jobId);
    JSONObject jobObject = new JSONObject();
    try {
      jobObject.put("name", state.getController().getName());
      jobObject.put("mapreduce_id", jobId);
      jobObject.put("updated_timestamp_ms", state.getMostRecentUpdateTimeMillis());
      jobObject.put("start_timestamp_ms", state.getStartTimeMillis());

      if (state.getStatus().isActive()) {
        jobObject.put("active", true);
      } else {
        jobObject.put("active", false);
        jobObject.put("result_status", "" + state.getStatus());
      }
      jobObject.put("shards", state.getTotalTaskCount());
      jobObject.put("active_shards", state.getActiveTaskCount());

      if (true /* detailed */) {
        WorkerResult<?> aggregateResult = state.getAggregateResult();
        jobObject.put("counters", toJson(aggregateResult.getCounters()));
        jobObject.put("chart_url",
            getChartUrl(state.getTotalTaskCount(),
                aggregateResult.getWorkerShardStates()));

        // HACK(ohler): We don't put the actual mapper parameters in here since
        // the Pipeline UI already shows them (in the toString() of the
        // MapReduceSpecification -- just need to make sure all Inputs and
        // Outputs have useful toString()s).  Instead, we put some other useful
        // info in here.
        JSONObject mapperParams = new JSONObject();
        mapperParams.put("Shards completed",
            state.getTotalTaskCount() - state.getActiveTaskCount());
        mapperParams.put("Shards active", state.getActiveTaskCount());
        mapperParams.put("Shards total", state.getTotalTaskCount());
        JSONObject mapperSpec = new JSONObject();
        mapperSpec.put("mapper_params", mapperParams);
        jobObject.put("mapper_spec", mapperSpec);

        JSONArray shardArray = new JSONArray();
        // Iterate in ascending order rather than the map's entrySet() order.
        for (int i = 0; i < state.getTotalTaskCount(); i++) {
          WorkerShardState shard = aggregateResult.getWorkerShardStates().get(i);
          JSONObject shardObject = new JSONObject();
          shardObject.put("shard_number", i);
          if (shard == null) {
            shardObject.put("active", false);
            shardObject.put("result_status", "initializing");
            shardObject.put("shard_description", ""); // TODO
          } else {
            boolean done = aggregateResult.getClosedWriters().get(i) != null;
            if (done) {
              shardObject.put("active", false);
              // result_status is only displayed if active is false.
              shardObject.put("result_status", "done");
            } else {
              shardObject.put("active", true);
            }
            shardObject.put("shard_description", ""); // TODO
            shardObject.put("updated_timestamp_ms", shard.getMostRecentUpdateTimeMillis());
            shardObject.put("last_work_item", shard.getLastWorkItem());
          }
          shardArray.put(shardObject);
        }
        jobObject.put("shards", shardArray);
      }
    } catch (JSONException e) {
      throw new RuntimeException("Hard coded string is null", e);
    }

    return jobObject;
  }

  private static JSONObject handleListJobs(HttpServletRequest request) {
    String cursor = request.getParameter("cursor");
    String countString = request.getParameter("count");
    int count = DEFAULT_JOBS_PER_PAGE_COUNT;
    if (countString != null) {
      count = Integer.parseInt(countString);
    }

    return handleListJobs(cursor, count);
  }

  /**
   * Handle the list_jobs AJAX command.
   */
  @SuppressWarnings("unused")
  private static JSONObject handleListJobs(String cursor, int count) {
    throw new RuntimeException("Not implemented");
  }
}
