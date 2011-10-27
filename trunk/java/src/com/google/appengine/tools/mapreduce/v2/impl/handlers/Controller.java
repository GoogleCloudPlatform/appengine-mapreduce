// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.HadoopCounterNames;
import com.google.appengine.tools.mapreduce.InvalidConfigurationException;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.QuotaManager;
import com.google.appengine.tools.mapreduce.util.Clock;
import com.google.appengine.tools.mapreduce.util.SystemClock;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * Mapper controller logic handler.
 *
 */
public class Controller {
  static Clock clock = new SystemClock();
  private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

  /**
   * Handle the abort_job AJAX command.
   */
  public static JSONObject handleAbortJob(String jobId) {
    // TODO(user): Implement
    return new JSONObject();
  }

  /**
   * Schedules a controller task queue invocation.
   *
   * @param context this MR's job context
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   * @param baseUrl
   */
  public static // VisibleForTesting
  void scheduleController(AppEngineJobContext context, int sliceNumber, String baseUrl) {
    String taskName = ("controller_" + context.getJobID() + "__" + sliceNumber).replace('_', '-');
    try {
      context.getControllerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(baseUrl + MapReduceServlet.CONTROLLER_PATH)
              .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, context.getJobID().toString())
              .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
              .countdownMillis(2000)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      MapReduceServlet.log.warning("Controller task " + taskName + " already exists.");
    }
  }

  /**
   * Handle the initial request to start the MapReduce.
   *
   * @return the JobID of the newly created MapReduce or {@code null} if the
   * MapReduce couldn't be created.
   */
  public static String handleStart(Configuration configuration, String name, String baseUrl) {
    AppEngineJobContext context = AppEngineJobContext.createContextForNewJob(configuration);

    // Initialize InputSplits
    Class<? extends InputFormat<?, ?>> inputFormatClass;
    try {
      inputFormatClass = context.getInputFormatClass();
    } catch (ClassNotFoundException e) {
      throw new InvalidConfigurationException("Invalid input format class specified.", e);
    }
    InputFormat<?, ?> inputFormat;
    try {
      inputFormat = inputFormatClass.newInstance();
    } catch (InstantiationException e) {
      throw new InvalidConfigurationException(
          "Input format class must have a default constructor.", e);
    } catch (IllegalAccessException e) {
      throw new InvalidConfigurationException(
          "Input format class must have a visible constructor.", e);
    }

    List<InputSplit> splits;
    try {
      splits = inputFormat.getSplits(context);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Thread got interrupted in a single-threaded environment. This shouldn't happen.", e);
    } catch (IOException e) {
      throw new RuntimeException(
          "Got an IOException while trying to make splits", e);
    }

    MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(
        ds, name, context.getJobID(), System.currentTimeMillis());

    mrState.setConfigurationXML(
        ConfigurationXmlUtil.convertConfigurationToXml(
            context.getConfiguration()));

    // Abort if we don't have any splits
    if (splits == null || splits.size() == 0) {
      mrState.setDone();
      mrState.persist();
      return null;
    }

    mrState.persist();
    scheduleController(context, 0, baseUrl);

    Worker.scheduleShards(context, inputFormat, splits, baseUrl);

    return mrState.getJobID();
  }

  public static void scheduleDoneCallback(Queue queue, String url, String jobId) {
    String taskName = ("done_callback" + jobId).replace('_', '-');
    try {
      queue.add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(url)
              .param("job_id", jobId)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      MapReduceServlet.log.warning("Done callback task " + taskName + " already exists.");
    }
  }

  public static void deleteAllShards(List<ShardState> shardStates) {
    List<Key> keys = new ArrayList<Key>();
    for (ShardState shardState : shardStates) {
      keys.add(shardState.getKey());
    }
    ds.delete(keys);
  }

  /**
   * Return all shards with status == ACTIVE.
   */
  public static List<ShardState> selectActiveShards(List<ShardState> shardStates) {
    List<ShardState> activeShardStates = new ArrayList<ShardState>();
    for (ShardState shardState : shardStates) {
      if (ShardState.Status.ACTIVE.equals(shardState.getStatus())) {
        activeShardStates.add(shardState);
      }
    }
    return activeShardStates;
  }

  /**
   * Refills quotas for all active shards based on the input processing rate.
   *
   * @param context context to get input processing rate from
   * @param mrState the MR state containing the last poll time
   * @param activeShardStates all active shard states
   */
  public static void refillQuotas(AppEngineJobContext context, MapReduceState mrState,
      List<ShardState> activeShardStates) {
    if (activeShardStates.size() == 0) {
      return;
    }

    long lastPollTime = mrState.getLastPollTime();
    long currentPollTime = clock.currentTimeMillis();

    int inputProcessingRate = context.getInputProcessingRate();
    long totalQuotaRefill;
    // Initial quota fill
    if (lastPollTime == -1) {
      totalQuotaRefill = inputProcessingRate;
    } else {
      long delta = currentPollTime - lastPollTime;
      totalQuotaRefill = (long) (delta * inputProcessingRate / 1000.0);
    }
    long perShardQuotaRefill = totalQuotaRefill / activeShardStates.size();

    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    for (ShardState activeShardState : activeShardStates) {
      manager.put(activeShardState.getTaskAttemptID().toString(), perShardQuotaRefill);
    }
    mrState.setLastPollTime(currentPollTime);
  }


  /**
   * Handles the logic for a controller task queue invocation.
   *
   * The primary jobs of the controller are to aggregate state from the
   * MR workers (e.g. presenting an overall view of the counters), and to set
   * the quota for MR workers.
   */
  public static void handleController(HttpServletRequest request) {
    AppEngineJobContext context = new AppEngineJobContext(request);
    try {
      List<ShardState> shardStates = ShardState.getShardStatesFromJobID(
          ds, context.getJobID());
      MapReduceState mrState = MapReduceState.getMapReduceStateFromJobID(
          ds, context.getJobID());

      List<ShardState> activeShardStates = selectActiveShards(shardStates);

      Controller.aggregateState(mrState, shardStates);
      mrState.setActiveShardCount(activeShardStates.size());
      mrState.setShardCount(shardStates.size());

      if (activeShardStates.size() == 0) {
        mrState.setDone();
      } else {
        refillQuotas(context, mrState, activeShardStates);
      }
      mrState.persist();

      if (MapReduceState.Status.ACTIVE.equals(mrState.getStatus())) {
        scheduleController(context, context.getSliceNumber() + 1, MapReduceServlet.getBase(request));
      } else {
        deleteAllShards(shardStates);
        if (context.hasDoneCallback()) {
          scheduleDoneCallback(
              context.getDoneCallbackQueue(), context.getDoneCallbackUrl(),
              context.getJobID().toString());
        }
      }
    } catch (EntityNotFoundException enfe) {
      MapReduceServlet.log.severe("Couldn't find the state for MapReduce: " + context.getJobID()
                 + ". Aborting!");
      return;
    }
  }

  /**
   * Update the current MR state by aggregating information from shard states.
   *
   * @param mrState the current MR state
   * @param shardStates all shard states (active and inactive)
   */
  public static void aggregateState(MapReduceState mrState, List<ShardState> shardStates) {
    List<Long> mapperCounts = new ArrayList<Long>();
    Counters counters = new Counters();
    for (ShardState shardState : shardStates) {
      Counters shardCounters = shardState.getCounters();
      // findCounter creates the counter if it doesn't exist.
      mapperCounts.add(shardCounters.findCounter(
          HadoopCounterNames.MAP_INPUT_RECORDS_GROUP,
          HadoopCounterNames.MAP_INPUT_RECORDS_NAME).getValue());

      for (CounterGroup shardCounterGroup : shardCounters) {
        for (Counter shardCounter : shardCounterGroup) {
          counters.findCounter(
              shardCounterGroup.getName(), shardCounter.getName()).increment(
                  shardCounter.getValue());
        }
      }
    }

    MapReduceServlet.log.fine("Aggregated counters: " + counters);
    mrState.setCounters(counters);
    mrState.setProcessedCounts(mapperCounts);
  }

}
