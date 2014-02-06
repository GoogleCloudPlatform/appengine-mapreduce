// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl.WORKER_PATH;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;
import com.google.appengine.api.backends.BackendService;
import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.labs.modules.ModulesService;
import com.google.appengine.api.labs.modules.ModulesServiceFactory;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.QueueStatistics;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetriesExhaustedException;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMapOutput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageReduceInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortOutput;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.MapReduceResultImpl;
import com.google.appengine.tools.mapreduce.impl.MapShardTask;
import com.google.appengine.tools.mapreduce.impl.ReduceShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.sort.SortContext;
import com.google.appengine.tools.mapreduce.impl.sort.SortShardTask;
import com.google.appengine.tools.mapreduce.impl.sort.SortWorker;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;


/**
 * A Pipeline job that runs a MapReduce.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 * @param <R> type of final result
 */
public class MapReduceJob<I, K, V, O, R>
    extends Job2<MapReduceResult<R>, MapReduceSpecification<I, K, V, O, R>, MapReduceSettings> {

  private static final long serialVersionUID = 723635736794527552L;
  private static final Logger log = Logger.getLogger(MapReduceJob.class.getName());

  private static final ExceptionHandler QUEUE_EXCEPTION_HANDLER =
      new ExceptionHandler.Builder().retryOn(TransientFailureException.class).build();

  /**
   * Starts a {@link MapReduceJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  public static <I, K, V, O, R> String start(
      MapReduceSpecification<I, K, V, O, R> specification, MapReduceSettings settings) {
    checkQueueSettings(settings.getWorkerQueueName());
    validateSpec(specification);
    settings = settings.clone();
    verifyAndSetBucketName(settings);
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(new MapReduceJob<I, K, V, O, R>(), specification,
        settings, makeJobSettings(settings));
  }

  private static void checkQueueSettings(String queueName) {
    final Queue queue = QueueFactory.getQueue(queueName);
    try {
      RetryHelper.runWithRetries(
          new Callable<QueueStatistics>() {
            @Override public QueueStatistics call() {
              return queue.fetchStatistics();
            }
          }, RetryParams.getDefaultInstance(), QUEUE_EXCEPTION_HANDLER);
    } catch (RetryHelperException ex) {
      if (ex.getCause() instanceof IllegalStateException) {
        throw new RuntimeException("Queue '" + queueName + "' does not exists");
      }
      throw new RuntimeException("Could not check if queue exists", ex.getCause());
    }
  }

  public MapReduceJob() {}

  @Override
  public String toString() {
    return getClass().getSimpleName() + "()";
  }

  @VisibleForTesting
  static ShardedJobSettings makeShardedJobSettings(
      String shardedJobId, MapReduceSettings mrSettings, Key pipelineKey) {
    String backend = mrSettings.getBackend();
    String module = mrSettings.getModule();
    String version = null;
    if (backend == null) {
      if (module == null) {
        BackendService backendService = BackendServiceFactory.getBackendService();
        String currentBackend = backendService.getCurrentBackend();
        // If currentBackend contains ':' it is actually a B type module (see b/12893879)
        if (currentBackend != null && currentBackend.indexOf(':') == -1) {
          backend = currentBackend;
        } else {
          ModulesService modulesService = ModulesServiceFactory.getModulesService();
          module = modulesService.getCurrentModule();
          version = modulesService.getCurrentVersion();
        }
      } else {
        ModulesService modulesService = ModulesServiceFactory.getModulesService();
        if (module.equals(modulesService.getCurrentModule())) {
          version = modulesService.getCurrentVersion();
        } else {
          // TODO(user): we may want to support providing a version for a module
          version = modulesService.getDefaultVersion(module);
        }
      }
    }
    return new ShardedJobSettings.Builder()
        .setControllerPath(mrSettings.getBaseUrl() + CONTROLLER_PATH + "/" + shardedJobId)
        .setWorkerPath(mrSettings.getBaseUrl() + WORKER_PATH + "/" + shardedJobId)
        .setMapReduceStatusUrl(mrSettings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId)
        .setPipelineStatusUrl(PipelineServlet.makeViewerUrl(pipelineKey, pipelineKey))
        .setBackend(backend)
        .setModule(module)
        .setVersion(version)
        .setQueueName(mrSettings.getWorkerQueueName())
        .setMaxShardRetries(mrSettings.getMaxShardRetries())
        .setMaxSliceRetries(mrSettings.getMaxSliceRetries())
        .setSliceTimeoutMillis(Math.max(ShardedJobSettings.DEFAULT_SLICE_TIMEOUT_MILLIS,
            (int) (mrSettings.getMillisPerSlice() * 1.1)))
        .build();
  }

  @VisibleForTesting
  static JobSetting[] makeJobSettings(MapReduceSettings mrSettings, JobSetting... extra) {
    JobSetting[] settings = new JobSetting[3 + extra.length];
    settings[0] = new JobSetting.OnBackend(mrSettings.getBackend());
    settings[1] = new JobSetting.OnModule(mrSettings.getModule());
    settings[2] = new JobSetting.OnQueue(mrSettings.getWorkerQueueName());
    System.arraycopy(extra, 0, settings, 3, extra.length);
    return settings;
  }

  private static class ResultAndStatus<R> implements Serializable {

    private static final long serialVersionUID = 7862563622882782696L;

    private final MapReduceResult<R> result;
    private final Status status;

    public ResultAndStatus(MapReduceResult<R> result, Status status) {
      this.result = result;
      this.status = status;
    }

    public MapReduceResult<R> getResult() {
      return result;
    }

    public Status getStatus() {
      return status;
    }
  }

  // TODO(user): This class will not be needed once b/11279055 is fixed.
  private static class ExamineStatusAndReturnResult<R>
      extends Job1<MapReduceResult<R>, ResultAndStatus<R>> {

    private static final long serialVersionUID = -4916783324594785878L;

    private final String stage;

    ExamineStatusAndReturnResult(String stage) {
      this.stage = stage;
    }

    @Override
    public Value<MapReduceResult<R>> run(ResultAndStatus<R> resultAndStatus) {
      Status status = resultAndStatus.getStatus();
      if (status.getStatusCode() == Status.StatusCode.DONE) {
        return immediate(resultAndStatus.getResult());
      }
      throw new MapReduceJobException(stage, status);
    }
  }

  private static class WorkerController<I, O, R, C extends WorkerContext<O>> extends
      ShardedJobController<WorkerShardTask<I, O, C>> {

    private static final long serialVersionUID = 931651840864967980L;

    private final Counters counters;
    private final Output<O, R> output;
    private final String resultPromiseHandle;

    WorkerController(String shardedJobName, Counters initialCounters, Output<O, R> output,
        String resultPromiseHandle) {
      super(shardedJobName);
      this.counters = checkNotNull(initialCounters, "Null counters");
      this.output = checkNotNull(output, "Null output");
      this.resultPromiseHandle = checkNotNull(resultPromiseHandle, "Null resultPromiseHandle");
    }

    @Override
    public void completed(List<? extends WorkerShardTask<I, O, C>> workers) {
      ImmutableList.Builder<OutputWriter<O>> outputWriters = ImmutableList.builder();
      for (WorkerShardTask<I, O, C> worker : workers) {
        outputWriters.add(worker.getOutputWriter());
        counters.addAll(worker.getContext().getCounters());
      }
      R outputResult;
      try {
        outputResult = output.finish(outputWriters.build());
      } catch (IOException e) {
        throw new RuntimeException(output + ".finish() threw IOException");
      }
      Status status = new Status(Status.StatusCode.DONE);
      ResultAndStatus<R> resultAndStatus = new ResultAndStatus<>(
          new MapReduceResultImpl<>(outputResult, counters), status);
      submitPromisedJob(resultAndStatus);
    }

    @Override
    public void failed(Status status) {
      submitPromisedJob(new ResultAndStatus<R>(null, status));
    }

    // TODO(user): consider using a pipeline for it after b/12067201 is fixed.
    private void submitPromisedJob(final ResultAndStatus<R> resultAndStatus) {
      try {
        PipelineServiceFactory.newPipelineService().submitPromisedValue(resultPromiseHandle,
            resultAndStatus);
      } catch (OrphanedObjectException e) {
        log.warning("Discarding an orphaned promiseHandle: " + resultPromiseHandle);
      } catch (NoSuchObjectException e) {
        // Let taskqueue retry.
        throw new RuntimeException(resultPromiseHandle + ": Handle not found, can't submit "
            + resultAndStatus + " going to retry.", e);
      }
    }
  }

  /**
   * The pipeline job to execute the Map phase of the MapReduce. (For all shards)
   */
  private static class MapJob<I, K, V> extends
      Job0<MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 274712180795282822L;

    private final String mrJobId;
    private final MapReduceSpecification<I, K, V, ?, ?> mrSpec;
    private final MapReduceSettings settings;
    private final String shardedJobId;

    private MapJob(
        String mrJobId, MapReduceSpecification<I, K, V, ?, ?> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
      shardedJobId = "map-" + mrJobId;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    /**
     * Starts a shardedJob for each map worker. Each receives a GoogleCloudStorageFileSet containing
     * a file per reduce shard ordered by reduce shard where data destined for that reduce shard
     * will be written. How this constructed and the format of the files is defined by
     * {@link GoogleCloudStorageMapOutput}.
     *
     * @returns A future containing the GoogleCloudStorageFileSets for all of the mappers. (Ordered
     *          by map shard number.)
     */
    @Override
    public Value<MapReduceResult<List<GoogleCloudStorageFileSet>>> run() {
      PromisedValue<ResultAndStatus<List<GoogleCloudStorageFileSet>>> resultAndStatus =
          newPromise();
      List<? extends InputReader<I>> readers;
      try {
        readers = mrSpec.getInput().createReaders();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      int reduceShardCount = mrSpec.getOutput().getNumShards();
      Output<KeyValue<K, V>, List<GoogleCloudStorageFileSet>> output =
          new GoogleCloudStorageMapOutput<>(settings.getBucketName(), mrJobId,
              readers.size(), mrSpec.getIntermediateKeyMarshaller(),
              mrSpec.getIntermediateValueMarshaller(), new HashingSharder(reduceShardCount));
      String shardedJobName = mrSpec.getJobName() + " (map phase)";
      List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters();
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> mapTasks =
          ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        mapTasks.add(new MapShardTask<>(mrJobId, i, readers.size(), readers.get(i),
            mrSpec.getMapper(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          makeShardedJobSettings(shardedJobId, settings, getPipelineKey());
      WorkerController<I, KeyValue<K, V>, List<GoogleCloudStorageFileSet>, MapperContext<K, V>>
          workerController = new WorkerController<>(
              shardedJobName, new CountersImpl(), output, resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, mapTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, makeJobSettings(settings));
      return futureCall(
          new ExamineStatusAndReturnResult<List<GoogleCloudStorageFileSet>>(shardedJobId),
          resultAndStatus, makeJobSettings(settings, waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl()), maxAttempts(1)));
    }

    @SuppressWarnings("unused")
    public Value<MapReduceResult<List<GoogleCloudStorageFileSet>>> handleException(
        CancellationException ex) {
      ShardedJobServiceFactory.getShardedJobService().abortJob(shardedJobId);
      return null;
    }
  }

  /**
   * The pipeline job to execute the Sort phase of the MapReduce. (For all shards)
   */
  private static class SortJob extends Job1<MapReduceResult<List<GoogleCloudStorageFileSet>>,
      MapReduceResult<List<GoogleCloudStorageFileSet>>> {

    private static final long serialVersionUID = 8761355950012542309L;
    // We don't need the CountersImpl part of the MapResult input here but we
    // accept it to avoid needing an adapter job to connect this job to MapJob's result.
    private final String mrJobId;
    private final MapReduceSpecification<?, ?, ?, ?, ?> mrSpec;
    private final MapReduceSettings settings;
    private final String shardedJobId;

    private SortJob(
        String mrJobId, MapReduceSpecification<?, ?, ?, ?, ?> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
      shardedJobId = "sort-" + mrJobId;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    /**
     * @param mapOutputs a list ordered by Map shard of sets containing one file per reduce shard.
     * @return a list ordered by Reduce shard of sets containing one file per map shard.
     */
    private static List<GoogleCloudStorageFileSet> transposeReaders(
        List<GoogleCloudStorageFileSet> mapOutputs, String bucket, int reduceShards) {
      List<GoogleCloudStorageFileSet> result = new ArrayList<>(reduceShards);
      for (int reduceShard = 0; reduceShard < reduceShards; reduceShard++) {
        List<String> reduceFiles = new ArrayList<>(mapOutputs.size());
        for (int mapShard = 0; mapShard < mapOutputs.size(); mapShard++) {
          reduceFiles.add(mapOutputs.get(mapShard).getFile(reduceShard).getObjectName());
        }
        result.add(new GoogleCloudStorageFileSet(bucket, reduceFiles));
      }
      return result;
    }

    /**
     * Takes in the the result of the map phase, and groups the files by the reducer they
     * are intended for. These files are then read, and written out in sorted order.
     * The result is a set of files for each reducer.
     * The format for how the data is written out is defined by {@link GoogleCloudStorageSortOutput}
     */
    @Override
    public Value<MapReduceResult<List<GoogleCloudStorageFileSet>>> run(
        MapReduceResult<List<GoogleCloudStorageFileSet>> mapResult) {
      PromisedValue<ResultAndStatus<List<GoogleCloudStorageFileSet>>> resultAndStatus =
          newPromise();
      int reduceShards = mrSpec.getOutput().getNumShards();
      List<GoogleCloudStorageFileSet> mapOutput =
          transposeReaders(mapResult.getOutputResult(), settings.getBucketName(), reduceShards);
      List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> readers =
          new GoogleCloudStorageSortInput(mapOutput).createReaders();
      Output<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, List<GoogleCloudStorageFileSet>> output =
          new GoogleCloudStorageSortOutput(settings.getBucketName(), mrJobId, reduceShards);
      String shardedJobName = mrSpec.getJobName() + " (sort phase)";
      List<? extends OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> writers =
          output.createWriters();

      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());

      ImmutableList.Builder<WorkerShardTask<
          KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          SortContext>> sortTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        sortTasks.add(new SortShardTask(mrJobId, i, readers.size(), readers.get(i),
            new SortWorker(), writers.get(i)));
      }
      ShardedJobSettings shardedJobSettings =
          makeShardedJobSettings(shardedJobId, settings, getPipelineKey());
      WorkerController<KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          List<GoogleCloudStorageFileSet>, SortContext> workerController = new WorkerController<>(
              shardedJobName, new CountersImpl(), output, resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, sortTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, makeJobSettings(settings));
      return futureCall(
          new ExamineStatusAndReturnResult<List<GoogleCloudStorageFileSet>>(shardedJobId),
          resultAndStatus, makeJobSettings(settings, waitFor(shardedJobResult),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl()), maxAttempts(1)));
    }

    @SuppressWarnings("unused")
    public Value<List<GoogleCloudStorageFileSet>> handleException(CancellationException ex) {
      ShardedJobServiceFactory.getShardedJobService().abortJob(shardedJobId);
      return null;
    }
  }

  /**
   * The pipeline job to execute the Reduce phase of the MapReduce. (For all shards)
   */
  private static class ReduceJob<K, V, O, R> extends Job2<MapReduceResult<R>,
      MapReduceResult<List<GoogleCloudStorageFileSet>>,
      MapReduceResult<List<GoogleCloudStorageFileSet>>> {

    private static final long serialVersionUID = 590237832617368335L;

    private final String mrJobId;
    private final MapReduceSpecification<?, K, V, O, R> mrSpec;
    private final MapReduceSettings settings;
    private final String shardedJobId;

    private ReduceJob(
        String mrJobId, MapReduceSpecification<?, K, V, O, R> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
      shardedJobId = "reduce-" + mrJobId;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    /**
     * Takes in the output from sort, and creates a sharded task to call the reducer with the
     * ordered input.
     * The way the data is read in is defined by {@link GoogleCloudStorageReduceInput}
     */
    @Override
    public Value<MapReduceResult<R>> run(MapReduceResult<List<GoogleCloudStorageFileSet>> mapResult,
        MapReduceResult<List<GoogleCloudStorageFileSet>> sortResult) {
      PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
      List<? extends InputReader<KeyValue<K, Iterator<V>>>> readers =
          new GoogleCloudStorageReduceInput<>(sortResult.getOutputResult(),
              mrSpec.getIntermediateKeyMarshaller(), mrSpec.getIntermediateValueMarshaller())
              .createReaders();
      String shardedJobName = mrSpec.getJobName() + " (reduce phase)";
      Output<O, R> output = mrSpec.getOutput();
      List<? extends OutputWriter<O>> writers = output.createWriters();

      Preconditions.checkArgument(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>
          reduceTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        reduceTasks.add(new ReduceShardTask<>(mrJobId, i, readers.size(), readers.get(i),
            mrSpec.getReducer(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          makeShardedJobSettings(shardedJobId, settings, getPipelineKey());
      WorkerController<KeyValue<K, Iterator<V>>, O, R, ReducerContext<O>> workerController =
          new WorkerController<>(shardedJobName, mapResult.getCounters(), output,
              resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, reduceTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, makeJobSettings(settings));
      return futureCall(new ExamineStatusAndReturnResult<R>(shardedJobId), resultAndStatus,
          makeJobSettings(settings, waitFor(shardedJobResult), maxAttempts(1),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
    }

    @SuppressWarnings("unused")
    public Value<MapReduceResult<R>> handleException(CancellationException ex) {
      ShardedJobServiceFactory.getShardedJobService().abortJob(shardedJobId);
      return null;
    }
  }

  /**
   * A sub-pipeline to delete intermediate data
   */
  private static class CleanupPipelineJob extends
      Job1<String, MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 354137030664235135L;

    private final String mrJobId;
    private final MapReduceSettings settings;

    private CleanupPipelineJob(String mrJobId, MapReduceSettings settings) {
      this.settings = settings;
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override
    public Value<String> run(MapReduceResult<List<GoogleCloudStorageFileSet>> files) {
      PipelineService service = PipelineServiceFactory.newPipelineService();
      return immediate(service.startNewPipeline(
          new CleanupJob(mrJobId, settings), files, makeJobSettings(settings)));
    }
  }

  /**
   * A Job to delete records of the pipeline it is in. (Used by CleanupPipelineJob)
   */
  private static class DeletePipelineJob extends Job0<Void> {

    private static final long serialVersionUID = 7957145050871420619L;
    private final String key;
    private final MapReduceSettings settings;

    private DeletePipelineJob(String key, MapReduceSettings settings) {
      this.key = key;
      this.settings = settings;
    }

    @Override
    public Value<Void> run() {
      DeferredTask deleteRecordsTask = new DeferredTask() {
        private static final long serialVersionUID = -7510918963650055768L;

        @Override
        public void run() {
          PipelineService service = PipelineServiceFactory.newPipelineService();
          try {
            service.deletePipelineRecords(key);
            log.info("Deleted pipeline: " + key);
          } catch (IllegalStateException e) {
            log.warning("Failed to delete pipeline: " + key);
            HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
            if (request != null && request.getIntHeader("X-AppEngine-TaskExecutionCount") < 3) {
              // TODO(user): replace exception with the line bellow once 1.9.0 is public
              // DeferredTaskContext.markForRetry();
              // Also when using markForRetry we could make the first delete attempt sooner
              // as retries will not be noisy.
              throw new RuntimeException("Retry deferred task", e);
            }
            try {
              service.deletePipelineRecords(key, true, false);
              log.info("Force deleted pipeline: " + key);
            } catch (Exception ex) {
              log.log(Level.WARNING, "Failed to force delete pipeline: " + key, ex);
            }
          } catch (NoSuchObjectException e) {
            // Already done
          }
        }
      };
      Queue queue = QueueFactory.getQueue(settings.getWorkerQueueName());
      queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(10000)
          .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(2).maxBackoffSeconds(10)));
      return null;
    }
  }


  /**
   * A Job that kicks off a CleanupFilesJob for each fileset it is provided.
   */
  private static class CleanupJob extends
      Job1<Void, MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 354137030664235135L;

    private final String mrJobId;
    private final MapReduceSettings settings;

    private CleanupJob(String mrJobId, MapReduceSettings settings) {
      this.settings = settings;
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    /**
     * Kicks off a job to delete each of the provided GoogleCloudStorageFileSets in parallel.
     */
    @Override
    public Value<Void> run(MapReduceResult<List<GoogleCloudStorageFileSet>> fileSet) {
      JobSetting[] waitForAll = new JobSetting[fileSet.getOutputResult().size()];
      int index = 0;
      for (GoogleCloudStorageFileSet files : fileSet.getOutputResult()) {
        FutureValue<Void> futureCall =
            futureCall(new CleanupFilesJob(mrJobId), immediate(files), makeJobSettings(settings));
        waitForAll[index++] = waitFor(futureCall);
      }
      // TODO(user): should not be needed once b/9940384 is fixed
      return futureCall(new DeletePipelineJob(getPipelineKey().getName(), settings),
          makeJobSettings(settings, waitForAll));
    }
  }

  /**
   * A job which deletes all the files in the provided GoogleCloudStorageFileSet
   */
  private static class CleanupFilesJob extends Job1<Void, GoogleCloudStorageFileSet> {
    private static final long serialVersionUID = 1386781994496334846L;
    private static final GcsService gcs =
        GcsServiceFactory.createGcsService(MapReduceConstants.GCS_RETRY_PARAMETERS);
    private final String mrJobId;

    private CleanupFilesJob(String mrJobId) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    /**
     * Deletes the files in the provided GoogleCloudStorageFileSet
     */
    @Override
    public Value<Void> run(GoogleCloudStorageFileSet files) throws Exception {
      for (GcsFilename file : files.getAllFiles()) {
        try {
          gcs.delete(file);
        } catch (RetriesExhaustedException e) {
          log.log(Level.WARNING, "Failed to cleanup file: " + file, e);
        } catch (IOException e) {
          log.log(Level.WARNING, "Failed to cleanup file: " + file, e);
        }
      }
      return null;
    }
  }

  @Override
  public Value<MapReduceResult<R>> run(
      MapReduceSpecification<I, K, V, O, R> mrSpec, MapReduceSettings settings) {
    validateSpec(mrSpec);
    verifyAndSetBucketName(settings);
    String mrJobId = getJobKey().getName();
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> mapResult = futureCall(
        new MapJob<>(mrJobId, mrSpec, settings), makeJobSettings(settings, maxAttempts(1)));
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> sortResult = futureCall(
        new SortJob(mrJobId, mrSpec, settings), mapResult,
        makeJobSettings(settings, maxAttempts(1)));
    futureCall(new CleanupPipelineJob(mrJobId, settings), mapResult,
        makeJobSettings(settings, waitFor(sortResult)));
    FutureValue<MapReduceResult<R>> reduceResult = futureCall(
        new ReduceJob<>(mrJobId, mrSpec, settings), mapResult, sortResult,
        makeJobSettings(settings, maxAttempts(1)));
    futureCall(new CleanupPipelineJob(mrJobId, settings), sortResult,
        makeJobSettings(settings, waitFor(reduceResult)));
    return reduceResult;
  }

  private static void validateSpec(MapReduceSpecification<?, ?, ?, ?, ?> mrSpec) {
    Preconditions.checkNotNull(mrSpec.getJobName());
    Preconditions.checkNotNull(mrSpec.getMapper());
    Preconditions.checkNotNull(mrSpec.getReducer());
    Preconditions.checkNotNull(mrSpec.getInput());
    Preconditions.checkNotNull(mrSpec.getOutput());
    Preconditions.checkNotNull(mrSpec.getIntermediateKeyMarshaller());
    Preconditions.checkNotNull(mrSpec.getIntermediateValueMarshaller());
    int numShards = mrSpec.getOutput().getNumShards();
    Preconditions.checkArgument(numShards > 0 && numShards <= 100,
        "Invalid number of reduce shards: " + numShards + " must be between 1 and 100.");
  }

  public Value<MapReduceResult<R>> handleException(Throwable t) throws Throwable {
    log.log(Level.SEVERE, "MapReduce job failed because of: ", t);
    throw t;
  }

  // TODO(user): Perhaps we should have some sort of generalized settings processing.
  private static void verifyAndSetBucketName(MapReduceSettings settings) {
    String bucket = settings.getBucketName();
    if (Strings.isNullOrEmpty(bucket)) {
      try {
        bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
        if (Strings.isNullOrEmpty(bucket)) {
          String message = "The BucketName property was not set in the MapReduceSettings object, "
              + "and this application does not have a default bucket configured to fall back on.";
          log.log(Level.SEVERE, message);
          throw new IllegalArgumentException(message);
        }
      } catch (AppIdentityServiceFailureException e) {
        throw new RuntimeException(
            "The BucketName property was not set in the MapReduceSettings object, "
            + "and could not get the default bucket.", e);
      }
      settings.setBucketName(bucket);
    }
    try {
      verifyBucketIsWritable(bucket);
    } catch (Exception e) {
      throw new RuntimeException("Writeable Bucket '" + bucket + "' test failed. See "
          + "http://developers.google.com/appengine/docs/java/googlecloudstorageclient/activate"
          + " for more information on how to setup Google Cloude storage.", e);
    }
  }

  private static void verifyBucketIsWritable(String bucket) throws IOException {
    GcsService gcsService = GcsServiceFactory.createGcsService();
    GcsFilename filename = new GcsFilename(bucket, UUID.randomUUID().toString() + ".tmp");
    if (gcsService.getMetadata(filename) != null) {
      log.warning("File '" + filename.getObjectName() + "' exists. Skipping bucket write test.");
      return;
    }
    try (GcsOutputChannel channel =
        gcsService.createOrReplace(filename, GcsFileOptions.getDefaultInstance())) {
      channel.write(ByteBuffer.wrap("Delete me!".getBytes(Charsets.UTF_8)));
    } finally {
      gcsService.delete(filename);
    }
  }
}
