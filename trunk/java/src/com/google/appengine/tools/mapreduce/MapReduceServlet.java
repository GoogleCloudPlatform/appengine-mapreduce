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

import com.google.appengine.api.datastore.Cursor;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
  private static final int DEFAULT_JOBS_PER_PAGE_COUNT = 50;

  private static final Logger log = Logger.getLogger(MapReduceServlet.class.getName());

  // Amount of time to spend on actual map() calls per task execution.
  private static final int PROCESSING_TIME_PER_TASK_MS = 10000;

  /**
   * Default amount of quota to divvy up per controller execution.
   */
  public static final int DEFAULT_QUOTA_BATCH_SIZE = 20;

  // VisibleForTesting
  static final String CONTROLLER_PATH = "controllerCallback";
  static final String START_PATH = "start";
  static final String MAPPER_WORKER_PATH = "mapperCallback";
  static final String COMMAND_PATH = "command";

  // Command paths
  static final String LIST_JOBS_PATH = "list_jobs";
  static final String LIST_CONFIGS_PATH = "list_configs";
  static final String CLEANUP_JOB_PATH = "cleanup_job";
  static final String ABORT_JOB_PATH = "abort_job";
  static final String GET_JOB_DETAIL_PATH = "get_job_detail";
  static final String START_JOB_PATH = "start_job";


  private DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
  private Clock clock = new SystemClock();

  /**
   * Returns the portion of the URL from the end of the TLD (exclusive) to the
   * handler portion (exclusive).
   *
   * For example, getBase(https://www.google.com/foo/bar) -> /foo/
   * However, there are handler portions that take more than segment
   * (currently only the command handlers). So in that case, we have:
   * getBase(https://www.google.com/foo/command/bar) -> /foo/
   */
  static String getBase(HttpServletRequest request) {
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
  private boolean checkForTaskQueue(HttpServletRequest request, HttpServletResponse response) {
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
  private boolean checkForAjax(HttpServletRequest request, HttpServletResponse response) {
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
      handleController(request, response);
    } else if (handler.startsWith(MAPPER_WORKER_PATH)) {
      if (!checkForTaskQueue(request, response)) {
        return;
      }
      handleMapperWorker(request, response);
    } else if (handler.startsWith(START_PATH)) {
      // We don't add a GET handler for this one, since we're expecting the user
      // to POST the whole XML specification.
      // TODO(user): Make name customizable.
      // TODO(user): Add ability to specify a redirect.
      handleStart(
          ConfigurationXmlUtil.getConfigurationFromXml(request.getParameter("configuration")),
          "Automatically run request", request);
    } else if (handler.startsWith(COMMAND_PATH)) {
      if (!checkForAjax(request, response)) {
        return;
      }
      handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
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
      handleCommand(handler.substring(COMMAND_PATH.length() + 1), request, response);
    } else {
      handleStaticResources(handler, response);
    }
  }

  /**
   * Handles all status page commands.
   */
  public void handleCommand(
      String command, HttpServletRequest request, HttpServletResponse response) {
    JSONObject retValue = null;
    response.setContentType("application/json");
    boolean isPost = "POST".equals(request.getMethod());
    try {
      if (command.equals(LIST_CONFIGS_PATH) && !isPost) {
        MapReduceXml xml;
        try {
          xml = MapReduceXml.getMapReduceXmlFromFile();
          retValue = handleListConfigs(xml);
        } catch (FileNotFoundException e) {
          retValue = new JSONObject();
          retValue.put("status", "Couldn't find mapreduce.xml file");
        }
      } else if (command.equals(LIST_JOBS_PATH) && !isPost) {
        String cursor = request.getParameter("cursor");
        String countString = request.getParameter("count");
        int count = DEFAULT_JOBS_PER_PAGE_COUNT;
        if (countString != null) {
          count = Integer.parseInt(countString);
        }

        retValue = handleListJobs(cursor, count);
      } else if (command.equals(CLEANUP_JOB_PATH) && isPost) {
        retValue = handleCleanupJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(ABORT_JOB_PATH) && isPost) {
        retValue = handleAbortJob(request.getParameter("mapreduce_id"));
      } else if (command.equals(GET_JOB_DETAIL_PATH) && !isPost) {
        retValue = handleGetJobDetail(request.getParameter("mapreduce_id"));
      } else if (command.equals(START_JOB_PATH) && isPost) {
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

  /**
   * Handle the list_configs AJAX command.
   */
  public JSONObject handleListConfigs(MapReduceXml xml) {
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
   * Handle the list_jobs AJAX command.
   */
  public JSONObject handleListJobs(String cursor, int count) {
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
   * Handle the cleanup_job AJAX command.
   */
  public JSONObject handleCleanupJob(String jobId) {
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
   * Handle the abort_job AJAX command.
   */
  public JSONObject handleAbortJob(String jobId) {
    // TODO(user): Implement
    return new JSONObject();
  }

  /**
   * Handle the get_job_detail AJAX command.
   */
  public JSONObject handleGetJobDetail(String jobId) {
    MapReduceState state;
    try {
      state = MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(jobId));
    } catch (EntityNotFoundException e) {
      throw new IllegalArgumentException("Couldn't find MapReduce for id:" + jobId, e);
    }
    return state.toJson(true);
  }

  /**
   * Handle the start_job AJAX command.
   */
  public JSONObject handleStartJob(Map<String, String> params, String name,
      HttpServletRequest request) {
    try {
      MapReduceXml mrXml = MapReduceXml.getMapReduceXmlFromFile();
      Configuration configuration = mrXml.instantiateConfiguration(name, params);
      // TODO(user): What should we be doing here for error handling?
      String jobId = handleStart(configuration, name, request);
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
   * Update the current MR state by aggregating information from shard states.
   *
   * @param mrState the current MR state
   * @param shardStates all shard states (active and inactive)
   */
  public void aggregateState(MapReduceState mrState, List<ShardState> shardStates) {
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

    log.fine("Aggregated counters: " + counters);
    mrState.setCounters(counters);
    mrState.setProcessedCounts(mapperCounts);
  }

  /**
   * Refills quotas for all active shards based on the input processing rate.
   *
   * @param context context to get input processing rate from
   * @param mrState the MR state containing the last poll time
   * @param activeShardStates all active shard states
   */
  public void refillQuotas(AppEngineJobContext context, MapReduceState mrState,
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
  public void handleController(HttpServletRequest request, HttpServletResponse response) {
    AppEngineJobContext context = new AppEngineJobContext(request, false);
    try {
      List<ShardState> shardStates = ShardState.getShardStatesFromJobID(
          ds, context.getJobID());
      MapReduceState mrState = MapReduceState.getMapReduceStateFromJobID(
          ds, context.getJobID());

      List<ShardState> activeShardStates = selectActiveShards(shardStates);

      aggregateState(mrState, shardStates);
      mrState.setActiveShardCount(activeShardStates.size());
      mrState.setShardCount(shardStates.size());

      if (activeShardStates.size() == 0) {
        mrState.setDone();
      } else {
        refillQuotas(context, mrState, activeShardStates);
      }
      mrState.persist();

      if (MapReduceState.Status.ACTIVE.equals(mrState.getStatus())) {
        scheduleController(request, context, context.getSliceNumber() + 1);
      } else {
        deleteAllShards(shardStates);
        if (context.hasDoneCallback()) {
          scheduleDoneCallback(
              context.getDoneCallbackQueue(), context.getDoneCallbackUrl(),
              context.getJobID().toString());
        }
      }
    } catch (EntityNotFoundException enfe) {
      log.severe("Couldn't find the state for MapReduce: " + context.getJobID()
                 + ". Aborting!");
      return;
    }
  }

  private void scheduleDoneCallback(Queue queue, String url, String jobId) {
    String taskName = ("done_callback" + jobId).replace('_', '-');
    try {
      queue.add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(url)
              .param("job_id", jobId)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Done callback task " + taskName + " already exists.");
    }
  }

  private void deleteAllShards(List<ShardState> shardStates) {
    List<Key> keys = new ArrayList<Key>();
    for (ShardState shardState : shardStates) {
      keys.add(shardState.getKey());
    }
    ds.delete(keys);
  }

  /**
   * Return all shards with status == ACTIVE.
   */
  private List<ShardState> selectActiveShards(List<ShardState> shardStates) {
    List<ShardState> activeShardStates = new ArrayList<ShardState>();
    for (ShardState shardState : shardStates) {
      if (ShardState.Status.ACTIVE.equals(shardState.getStatus())) {
        activeShardStates.add(shardState);
      }
    }
    return activeShardStates;
  }

  /**
   * Process one task invocation worth of
   * {@link AppEngineMapper#map(Object, Object, org.apache.hadoop.mapreduce.Mapper.Context)}
   * calls. Also handles calling
   * {@link AppEngineMapper#taskSetup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * and
   * {@link AppEngineMapper#taskCleanup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * before and after any map calls made.
   *
   * @return
   * @throws IOException if the provided {@code mapper} throws such an exception
   * during execution
   *
   * @throws InterruptedException if the provided {@code mapper} throws such an
   * exception during execution
   */
  // VisibleForTesting
  <INKEY,INVALUE,OUTKEY,OUTVALUE>
  boolean processMapper(
      AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper,
      Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context context,
      QuotaConsumer consumer,
      long startTime)
      throws IOException, InterruptedException {
    boolean shouldShardContinue = true;
    if (consumer.check(1)) {
      mapper.taskSetup(context);
      while (clock.currentTimeMillis() < startTime + PROCESSING_TIME_PER_TASK_MS
          && consumer.consume(1)
          && (shouldShardContinue = context.nextKeyValue())) {
        mapper.map(context.getCurrentKey(), context.getCurrentValue(), context);

        Counter inputRecordsCounter = context.getCounter(
            HadoopCounterNames.MAP_INPUT_RECORDS_GROUP,
            HadoopCounterNames.MAP_INPUT_RECORDS_NAME);
        inputRecordsCounter.increment(1);
      }
      mapper.taskCleanup(context);
    } else {
      log.info("Out of mapper quota. Aborting request until quota is replenished."
          + " Consider increasing " + AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY
          + " (default " + AppEngineJobContext.DEFAULT_MAP_INPUT_PROCESSING_RATE
          + ") if you would like your mapper job to complete faster.");
    }

    return shouldShardContinue;
  }

  /**
   * Does a single task queue invocation's worth of worker work. Also handles
   * calling
   * {@link AppEngineMapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * and
   * {@link AppEngineMapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * as appropriate.
   */
  public <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void handleMapperWorker(HttpServletRequest request, HttpServletResponse response) {
    AppEngineJobContext jobContext = new AppEngineJobContext(request, false);
    AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
        request, jobContext, ds);

    if (taskAttemptContext.getShardState() == null) {
      // Shard state has vanished. This is probably the task being executed
      // out of order by taskqueue.
      log.warning("Shard state not found, aborting: " + taskAttemptContext.getTaskAttemptID() + " "
          + jobContext.getSliceNumber());
      return;
    }

    if (taskAttemptContext.getShardState().getStatus() != ShardState.Status.ACTIVE) {
      // Shard is not in an active state. This is probably the task being executed
      // out of order by taskqueue.
      log.warning("Shard is not active, aborting: " + taskAttemptContext.getTaskAttemptID() + " "
          + jobContext.getSliceNumber());
      return;
    }

    long startTime = clock.currentTimeMillis();
    log.fine("Running worker: " + taskAttemptContext.getTaskAttemptID() + " "
        + jobContext.getSliceNumber());

    try {
      AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
        taskAttemptContext.<INKEY,INVALUE,OUTKEY,OUTVALUE>getMapper();
      InputSplit split = taskAttemptContext.getInputSplit();
      RecordReader<INKEY, INVALUE> reader =
          taskAttemptContext.<INKEY, INVALUE>getRecordReader(split);
      DatastorePersistingStatusReporter reporter =
          new DatastorePersistingStatusReporter(taskAttemptContext.getShardState());
      AppEngineMapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.AppEngineContext context = getMapperContext(
          taskAttemptContext, mapper, split, reader, reporter);

      if (jobContext.getSliceNumber() == 0) {
        // This is the first invocation for this mapper.
        mapper.setup(context);
      }

      QuotaConsumer consumer = getQuotaConsumer(taskAttemptContext);

      boolean shouldContinue = processMapper(mapper, context, consumer, startTime);

      if (shouldContinue) {
        taskAttemptContext.getShardState().setRecordReader(jobContext.getConfiguration(), reader);
      } else {
        taskAttemptContext.getShardState().setDone();
      }

      // This persists the shard state including the new record reader.
      reporter.persist();

      consumer.dispose();

      if (shouldContinue) {
        scheduleWorker(
            request, jobContext, context.getTaskAttemptID(), jobContext.getSliceNumber() + 1);
      } else {
        // This is the last invocation for this mapper.
        mapper.cleanup(context);
      }
    } catch (IOException ioe) {
      // TODO(user): Currently all user errors result in retry. We should
      // figure out some way to differentiate which should be fatal (or
      // maybe just have a memcache counter for each shard that causes us
      // to bail on repeated errors).
      throw new RuntimeException(ioe);
    } catch (SecurityException e) {
      throw new RuntimeException(
          "MapReduce framework doesn't have permission to instantiate classes.", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Got exception instantiating Mapper.Context", e);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Got InterruptedException running Mapper. This should never happen.", e);
    }
  }

  /**
   * Get the QuotaConsumer for current shard.
   */
  private QuotaConsumer getQuotaConsumer(AppEngineTaskAttemptContext taskAttemptContext) {
    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    QuotaConsumer consumer = new QuotaConsumer(
        manager, taskAttemptContext.getTaskAttemptID().toString(), DEFAULT_QUOTA_BATCH_SIZE);
    return consumer;
  }

  /**
   * Get the mapper context for the current shard. Since there is currently
   * no reducer support, the output values are currently set to {@code null}.
   *
   * @return the newly initialized context
   * @throws InvocationTargetException if the constructor throws an exception
   */
  @SuppressWarnings("unchecked")
  private <INKEY, INVALUE, OUTKEY, OUTVALUE>
  AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.AppEngineContext getMapperContext(
      AppEngineTaskAttemptContext taskAttemptContext,
      AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper,
      InputSplit split,
      RecordReader<INKEY, INVALUE> reader,
      StatusReporter reporter) throws InvocationTargetException {
    Constructor<AppEngineMapper.AppEngineContext> contextConstructor;
    try {
      contextConstructor = AppEngineMapper.AppEngineContext.class.getConstructor(
        new Class[]{
            AppEngineMapper.class,
            Configuration.class,
            TaskAttemptID.class,
            RecordReader.class,
            RecordWriter.class,
            OutputCommitter.class,
            StatusReporter.class,
            InputSplit.class
        }
      );
      AppEngineMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.AppEngineContext context =
          contextConstructor.newInstance(
              mapper,
              taskAttemptContext.getConfiguration(),
              taskAttemptContext.getTaskAttemptID(),
              reader,
              null, /* not yet implemented */
              null, /* not yet implemented */
              reporter,
              split
      );
      return context;
    } catch (SecurityException e) {
      // Since we know the class we're calling, this is strictly a programming error.
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (NoSuchMethodException e) {
      // Same
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (IllegalArgumentException e) {
      // There's a small chance this could be a bad supplied argument,
      // but we should validate that earlier.
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (InstantiationException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    } catch (IllegalAccessException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Mapper.Context", e);
    }

  }

  // VisibleForTesting
  void setClock(Clock clock) {
    this.clock = clock;
  }

  /**
   * Handle the initial request to start the MapReduce.
   *
   * @return the JobID of the newly created MapReduce or {@code null} if the
   * MapReduce couldn't be created.
   */
  public String handleStart(Configuration conf, String name, HttpServletRequest request) {
    AppEngineJobContext context = new AppEngineJobContext(conf, request, true);

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
    scheduleController(request, context, 0);

    scheduleShards(request, context, inputFormat, splits);

    return mrState.getJobID();
  }

  /**
   * Schedules a controller task queue invocation.
   *
   * @param req the current request
   * @param context this MR's job context
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   */
  // VisibleForTesting
  void scheduleController(HttpServletRequest req, AppEngineJobContext context, int sliceNumber) {
    String taskName = ("controller_" + context.getJobID() + "__" + sliceNumber).replace('_', '-');
    try {
      context.getControllerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(getBase(req) + CONTROLLER_PATH)
              .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, context.getJobID().toString())
              .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
              .countdownMillis(2000)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Controller task " + taskName + " already exists.");
    }
  }

  /**
   * Schedules a worker task on the appropriate queue.
   *
   * @param req the current servlet request
   * @param context the context for this MR job
   * @param taskAttemptId the task attempt ID for this worker
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   */
  // VisibleForTesting
  void scheduleWorker(HttpServletRequest req, AppEngineJobContext context,
      TaskAttemptID taskAttemptId, int sliceNumber) {
    Preconditions.checkArgument(
        context.getJobID().equals(taskAttemptId.getJobID()),
        "Worker task must be for this MR job");

    String taskName = ("worker_" + taskAttemptId + "__" + sliceNumber).replace('_', '-');
    try {
      context.getWorkerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(getBase(req) + MAPPER_WORKER_PATH)
              .param(AppEngineTaskAttemptContext.TASK_ATTEMPT_ID_PARAMETER_NAME,
                  "" + taskAttemptId)
              .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, "" + taskAttemptId.getJobID())
              .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      log.warning("Worker task " + taskName + " already exists.");
    }
  }

  /**
   * Schedules the initial worker callback execution for all shards.
   *
   * @param req the current request
   * @param context this MR's context
   * @param format the input format to use for generating {@code RecordReader}s
   * from the {@code InputSplit}s
   * @param splits all input splits for this MR
   */
  // VisibleForTesting
  void scheduleShards(HttpServletRequest req, AppEngineJobContext context,
      InputFormat<?,?> format, List<InputSplit> splits) {
    // TODO(user): To make life easy for people using InputFormats
    // from general Hadoop, we should add support for grouping
    // InputFormats that generate many splits into a reasonable
    // number of shards.

    // TODO(user): We will pass along the configuration so that worker tasks
    // don't have to read the MapReduceState whenever task queue supports
    // reasonable size payloads.

    int i = 0;
    for (InputSplit split : splits) {
      Configuration conf = context.getConfiguration();
      TaskAttemptID taskAttemptId = new TaskAttemptID(
          new TaskID(context.getJobID(), true, i), 1);
      ShardState shardState = ShardState.generateInitializedShardState(ds, taskAttemptId);
      shardState.setInputSplit(conf, split);
      AppEngineTaskAttemptContext attemptContext = new AppEngineTaskAttemptContext(
          context, shardState, taskAttemptId);
      try {
        RecordReader<?,?> reader = format.createRecordReader(split, attemptContext);
        shardState.setRecordReader(conf, reader);
      } catch (IOException e) {
        throw new RuntimeException(
            "Got an IOException creating a record reader.", e);
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Got an interrupted exception in a single threaded environment.", e);
      }
      shardState.persist();
      scheduleWorker(req, context, taskAttemptId, 0);
      i++;
    }
  }

  /**
   * Handle serving of static resources (which we do dynamically so users
   * only have to add one entry to their web.xml).
   */
  public void handleStaticResources(String handler, HttpServletResponse response) {
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
