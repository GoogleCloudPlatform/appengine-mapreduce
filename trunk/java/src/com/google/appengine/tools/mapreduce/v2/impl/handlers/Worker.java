// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.TaskAlreadyExistsException;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.google.appengine.tools.mapreduce.AppEngineTaskAttemptContext;
import com.google.appengine.tools.mapreduce.DatastorePersistingStatusReporter;
import com.google.appengine.tools.mapreduce.HadoopCounterNames;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.QuotaConsumer;
import com.google.appengine.tools.mapreduce.QuotaManager;
import com.google.appengine.tools.mapreduce.util.Clock;
import com.google.appengine.tools.mapreduce.util.SystemClock;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;
import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * Mapper Worker logic handler.
 *
 */
public class Worker {
  private static Clock clock = new SystemClock();
  private static DatastoreService ds = DatastoreServiceFactory.getDatastoreService();

  /**
   * Schedules a worker task on the appropriate queue.
   *
   * @param context the context for this MR job
   * @param taskAttemptId the task attempt ID for this worker
   * @param sliceNumber a counter that increments for each sequential, successful
   * task queue invocation
   * @param baseUrl
   */
  public static // VisibleForTesting
  void scheduleWorker(AppEngineJobContext context, TaskAttemptID taskAttemptId, int sliceNumber, String baseUrl) {
    Preconditions.checkArgument(
        context.getJobID().equals(taskAttemptId.getJobID()),
        "Worker task must be for this MR job");

    String taskName = ("worker_" + taskAttemptId + "__" + sliceNumber).replace('_', '-');
    try {
      context.getWorkerQueue().add(
          TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
              .url(baseUrl + MapReduceServlet.MAPPER_WORKER_PATH)
              .param(AppEngineTaskAttemptContext.TASK_ATTEMPT_ID_PARAMETER_NAME,
                  "" + taskAttemptId)
              .param(AppEngineJobContext.JOB_ID_PARAMETER_NAME, "" + taskAttemptId.getJobID())
              .param(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME, "" + sliceNumber)
              .taskName(taskName));
    } catch (TaskAlreadyExistsException e) {
      MapReduceServlet.log.warning("Worker task " + taskName + " already exists.");
    }
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
  public static
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
      while (clock.currentTimeMillis() < startTime + MapReduceServlet.PROCESSING_TIME_PER_TASK_MS
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
      MapReduceServlet.log.info("Out of mapper quota. Aborting request until quota is replenished."
          + " Consider increasing " + AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY
          + " (default " + AppEngineJobContext.DEFAULT_MAP_INPUT_PROCESSING_RATE
          + ") if you would like your mapper job to complete faster.");
    }

    return shouldShardContinue;
  }

  /**
   * Get the mapper context for the current shard. Since there is currently
   * no reducer support, the output values are currently set to {@code null}.
   *
   * @return the newly initialized context
   * @throws InvocationTargetException if the constructor throws an exception
   */
  @SuppressWarnings("unchecked")
  public
  static <INKEY, INVALUE, OUTKEY, OUTVALUE>
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

  /**
   * Schedules the initial worker callback execution for all shards.
   *
   * @param context this MR's context
   * @param format the input format to use for generating {@code RecordReader}s
   * from the {@code InputSplit}s
   * @param splits all input splits for this MR
   * @param baseUrl
   */
  public static // VisibleForTesting
  void scheduleShards(AppEngineJobContext context,
      InputFormat<?, ?> format, List<InputSplit> splits, String baseUrl) {
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
      scheduleWorker(context, taskAttemptId, 0, baseUrl);
      i++;
    }
  }

  /**
   * Does a single task queue invocation's worth of worker work. Also handles
   * calling
   * {@link AppEngineMapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * and
   * {@link AppEngineMapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)}
   * as appropriate.
   */
  public static <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void handleMapperWorker(HttpServletRequest request) {
    AppEngineJobContext jobContext = new AppEngineJobContext(request);
    AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
        request, jobContext, ds);

    if (taskAttemptContext.getShardState() == null) {
      // Shard state has vanished. This is probably the task being executed
      // out of order by taskqueue.
      MapReduceServlet.log.warning("Shard state not found, aborting: " + taskAttemptContext.getTaskAttemptID() + " "
          + jobContext.getSliceNumber());
      return;
    }

    if (taskAttemptContext.getShardState().getStatus() != ShardState.Status.ACTIVE) {
      // Shard is not in an active state. This is probably the task being executed
      // out of order by taskqueue.
      MapReduceServlet.log.warning("Shard is not active, aborting: " + taskAttemptContext.getTaskAttemptID() + " "
          + jobContext.getSliceNumber());
      return;
    }

    long startTime = clock.currentTimeMillis();
    MapReduceServlet.log.fine("Running worker: " + taskAttemptContext.getTaskAttemptID() + " "
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

      QuotaConsumer consumer = Worker.getQuotaConsumer(taskAttemptContext);

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
            jobContext, context.getTaskAttemptID(), jobContext.getSliceNumber() + 1,
            MapReduceServlet.getBase(request));
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
  public static QuotaConsumer getQuotaConsumer(AppEngineTaskAttemptContext taskAttemptContext) {
    QuotaManager manager = new QuotaManager(MemcacheServiceFactory.getMemcacheService());
    QuotaConsumer consumer = new QuotaConsumer(
        manager, taskAttemptContext.getTaskAttemptID().toString(), MapReduceServlet.DEFAULT_QUOTA_BATCH_SIZE);
    return consumer;
  }

}
