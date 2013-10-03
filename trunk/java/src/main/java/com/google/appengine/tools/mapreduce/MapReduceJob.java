// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetriesExhaustedException;
import com.google.appengine.tools.mapreduce.impl.AbstractWorkerController;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMapOutput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageReduceInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortOutput;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.MapReduceResultImpl;
import com.google.appengine.tools.mapreduce.impl.MapShardTask;
import com.google.appengine.tools.mapreduce.impl.ReduceShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerResult;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.sort.SortContext;
import com.google.appengine.tools.mapreduce.impl.sort.SortShardTask;
import com.google.appengine.tools.mapreduce.impl.sort.SortWorker;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

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

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(MapReduceJob.class.getName());

  /**
   * Starts a {@link MapReduceJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  public static <I, K, V, O, R> String start(
      MapReduceSpecification<I, K, V, O, R> specification, MapReduceSettings settings) {
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(new MapReduceJob<I, K, V, O, R>(), specification,
        settings, Job.onBackend(settings.getBackend()));
  }

  public MapReduceJob() {}

  @Override
  public String toString() {
    return getClass().getSimpleName() + "()";
  }

  private static ShardedJobSettings makeShardedJobSettings(MapReduceSettings mrSettings) {
    return new ShardedJobSettings().setControllerPath(
        mrSettings.getBaseUrl() + MapReduceServletImpl.CONTROLLER_PATH)
        .setWorkerPath(mrSettings.getBaseUrl() + MapReduceServletImpl.WORKER_PATH)
        .setControllerBackend(mrSettings.getBackend())
        .setWorkerBackend(mrSettings.getBackend())
        .setControllerQueueName(mrSettings.getControllerQueueName())
        .setWorkerQueueName(mrSettings.getWorkerQueueName());
  }
  // TODO(user) b/9693832
  private static class FillPromiseJob extends Job2<Void, String, Object> {
    private static final long serialVersionUID = 850701484460334898L;

    FillPromiseJob() {}

    @Override
    public Value<Void> run(String promiseHandle, Object value) {
      try {
        PipelineServiceFactory.newPipelineService().submitPromisedValue(promiseHandle, value);
      } catch (OrphanedObjectException e) {
        throw new RuntimeException(
            promiseHandle + ": Object orphaned, can't submit result " + value, e);
      } catch (NoSuchObjectException e) {
        // TODO(ohler): This exception is retryable, we should retry here rather
        // than through the task queue.
        throw new RuntimeException(
            promiseHandle + ": Handle not found, can't submit result " + value, e);
      }
      return immediate(null);
    }
  }

  private static class WorkerController<I, O, R, C extends WorkerContext>
      extends AbstractWorkerController<WorkerShardTask<I, O, C>, O> {
    private static final long serialVersionUID = 931651840864967980L;

    private final Counters initialCounters;
    private final Output<O, R> output;
    private final String resultPromiseHandle;

    WorkerController(String shardedJobName, Counters initialCounters, Output<O, R> output,
        String resultPromiseHandle) {
      super(shardedJobName);
      this.initialCounters = checkNotNull(initialCounters, "Null initialCounters");
      this.output = checkNotNull(output, "Null output");
      this.resultPromiseHandle = checkNotNull(resultPromiseHandle, "Null resultPromiseHandle");
    }

    @Override
    public void completed(WorkerResult<O> finalCombinedResult) {
      R outputResult;
      try {
        outputResult = output.finish(finalCombinedResult.getClosedWriters().values());
      } catch (IOException e) {
        throw new RuntimeException(output + ".finish() threw IOException");
      }
      Counters totalCounters = new CountersImpl();
      totalCounters.addAll(initialCounters);
      totalCounters.addAll(finalCombinedResult.getCounters());
      MapReduceResult<R> result = new MapReduceResultImpl<R>(outputResult, totalCounters);
      // TODO(user) b/9693832
      PipelineServiceFactory.newPipelineService()
          .startNewPipeline(new FillPromiseJob(), resultPromiseHandle, result);
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

    private MapJob(
        String mrJobId, MapReduceSpecification<I, K, V, ?, ?> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
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
      @SuppressWarnings({"unchecked", "rawtypes"})
      PromisedValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> result =
          (PromisedValue) newPromise(MapReduceResult.class);
      String shardedJobId = mrJobId + "-map";
      List<? extends InputReader<I>> readers;
      try {
        readers = mrSpec.getInput().createReaders();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      int reduceShardCount = mrSpec.getOutput().getNumShards();
      Output<KeyValue<K, V>, List<GoogleCloudStorageFileSet>> output =
          new GoogleCloudStorageMapOutput<K, V>(getAndSaveBucketName(settings),
              mrJobId,
              readers.size(),
              mrSpec.getIntermediateKeyMarshaller(),
              mrSpec.getIntermediateValueMarshaller(),
              new HashingSharder(reduceShardCount));
      String shardedJobName = mrSpec.getJobName() + " (map phase)";
      List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters();

      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> mapTasks =
          ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        mapTasks.add(new MapShardTask<I, K, V>(mrJobId,
            i,
            readers.size(),
            readers.get(i),
            mrSpec.getMapper(),
            writers.get(i),
            settings.getMillisPerSlice()));
      }
      ShardedJobServiceFactory.getShardedJobService().startJob(shardedJobId,
          mapTasks.build(), new WorkerController<
              I, KeyValue<K, V>, List<GoogleCloudStorageFileSet>, MapperContext<K, V>>(
              shardedJobName, new CountersImpl(), output, result.getHandle()),
          makeShardedJobSettings(settings));
      setStatusConsoleUrl(settings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId);
      return result;
    }
  }

  /**
   * The pipeline job to execute the Sort phase of the MapReduce. (For all shards)
   */
  private static class SortJob extends Job1<MapReduceResult<List<GoogleCloudStorageFileSet>>,
      MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 8761355950012542309L;
    // We don't need the CountersImpl part of the MapResult input here but we
    // accept it to avoid needing an adapter job to connect this job to MapJob's
    // result.
    private final String mrJobId;
    private final MapReduceSpecification<?, ?, ?, ?, ?> mrSpec;
    private final MapReduceSettings settings;

    private SortJob(
        String mrJobId, MapReduceSpecification<?, ?, ?, ?, ?> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
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
      List<GoogleCloudStorageFileSet> result =
          new ArrayList<GoogleCloudStorageFileSet>(reduceShards);
      for (int reduceShard = 0; reduceShard < reduceShards; reduceShard++) {
        List<String> reduceFiles = new ArrayList<String>();
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
      @SuppressWarnings({"unchecked", "rawtypes"})
      PromisedValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> result =
          (PromisedValue) newPromise(MapReduceResult.class);
      String shardedJobId = mrJobId + "-sort";
      int reduceShards = mrSpec.getOutput().getNumShards();
      String bucket = getAndSaveBucketName(settings);
      List<GoogleCloudStorageFileSet> mapOutput =
          transposeReaders(mapResult.getOutputResult(), bucket, reduceShards);
      List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> readers =
          new GoogleCloudStorageSortInput(mapOutput).createReaders();
      Output<KeyValue<ByteBuffer, Iterator<ByteBuffer>>, List<GoogleCloudStorageFileSet>> output =
          new GoogleCloudStorageSortOutput(bucket, mrJobId, reduceShards);
      String shardedJobName = mrSpec.getJobName() + " (sort phase)";
      List<? extends OutputWriter<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> writers =
          output.createWriters();

      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());

      ImmutableList.Builder<WorkerShardTask<
          KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
          SortContext>> sortTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        sortTasks.add(new SortShardTask(mrJobId,
            i,
            readers.size(),
            readers.get(i),
            new SortWorker(),
            writers.get(i)));
      }
      ShardedJobServiceFactory.getShardedJobService().startJob(shardedJobId,
          sortTasks.build(), new WorkerController<
              KeyValue<ByteBuffer, ByteBuffer>, KeyValue<ByteBuffer, Iterator<ByteBuffer>>,
              List<GoogleCloudStorageFileSet>, SortContext>(
              shardedJobName, new CountersImpl(), output, result.getHandle()),
          makeShardedJobSettings(settings));
      setStatusConsoleUrl(settings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId);
      return result;
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

    private ReduceJob(
        String mrJobId, MapReduceSpecification<?, K, V, O, R> mrSpec, MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
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
      @SuppressWarnings({"unchecked", "rawtypes"})
      PromisedValue<MapReduceResult<R>> result = (PromisedValue) newPromise(MapReduceResult.class);
      List<? extends InputReader<KeyValue<K, Iterator<V>>>> readers =
          new GoogleCloudStorageReduceInput<K, V>(sortResult.getOutputResult(),
              mrSpec.getIntermediateKeyMarshaller(), mrSpec.getIntermediateValueMarshaller())
              .createReaders();
      String shardedJobId = mrJobId + "-reduce";
      String shardedJobName = mrSpec.getJobName() + " (reduce phase)";
      Output<O, R> output = mrSpec.getOutput();
      List<? extends OutputWriter<O>> writers = output.createWriters();

      Preconditions.checkArgument(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>
          sortTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        sortTasks.add(new ReduceShardTask<K, V, O>(mrJobId,
            i,
            readers.size(),
            readers.get(i),
            mrSpec.getReducer(),
            writers.get(i),
            settings.getMillisPerSlice()));
      }
      ShardedJobServiceFactory.getShardedJobService().startJob(shardedJobId,
          sortTasks.build(), new WorkerController<
              KeyValue<K, Iterator<V>>, O, R, ReducerContext<O>>(
              shardedJobName, mapResult.getCounters(), output, result.getHandle()),
          makeShardedJobSettings(settings));
      setStatusConsoleUrl(settings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId);
      return result;
    }
  }

  /**
   * A sub-pipeline to delete intermediate data
   */
  private static class CleanupPipelineJob extends
      Job1<String, MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 354137030664235135L;

    private final String mrJobId;
    private MapReduceSettings settings;

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
      return immediate(service.startNewPipeline(new CleanupJob(mrJobId, settings), files));
    }
  }

  /**
   * A Job to delete records of the pipeline it is in. (Used by CleanupPipelineJob)
   */
  private static class DeletePipelineJob extends Job1<Void, List<Void>> {
    private static final long serialVersionUID = 7957145050871420619L;
    private String key;
    private final MapReduceSettings settings;

    private DeletePipelineJob(String key, MapReduceSettings settings) {
      this.key = key;
      this.settings = settings;
    }

    @Override
    public Value<Void> run(List<Void> param1) throws Exception {
      DeferredTask deleteRecordsTask = new DeferredTask() {
        private static final long serialVersionUID = -7510918963650055768L;

        @Override
        public void run() {
          PipelineService service = PipelineServiceFactory.newPipelineService();
          try {
            service.deletePipelineRecords(key, false, false);
            log.info("Deleted pipeline: " + key);
          } catch (IllegalStateException e) {
            log.info("Failed to delete pipeline: " + key + " retrying in 5 minutes.");
            Queue queue = QueueFactory.getQueue(settings.getControllerQueueName());
            queue.add(TaskOptions.Builder.withPayload(this).countdownMillis(300 * 1000)
                .retryOptions(RetryOptions.Builder.withTaskRetryLimit(0)));
          } catch (NoSuchObjectException e) {
            // Already done
          }
        }
      };
      Queue queue = QueueFactory.getQueue(settings.getControllerQueueName());
      queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(300 * 1000)
          .retryOptions(RetryOptions.Builder.withTaskRetryLimit(0)));
      return immediate(null);
    }
  }

  /**
   * A Job that kicks off a CleanupFilesJob for each fileset it is provided.
   */
  private static class CleanupJob extends
      Job1<Void, MapReduceResult<List<GoogleCloudStorageFileSet>>> {
    private static final long serialVersionUID = 354137030664235135L;

    private final String mrJobId;
    private MapReduceSettings settings;

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
      ArrayList<FutureValue<Void>> results = new ArrayList<FutureValue<Void>>();
      for (GoogleCloudStorageFileSet files : fileSet.getOutputResult()) {
        FutureValue<Void> futureCall = futureCall(new CleanupFilesJob(mrJobId), immediate(files),
            Job.onBackend(settings.getBackend()));
        results.add(futureCall);
      }
      // Passing a list because b/10728565
      return futureCall(new DeletePipelineJob(this.getPipelineKey().getName(), settings),
          futureList(results));
    }
  }

  /**
   * A job which deletes all the files in the provided GoogleCloudStorageFileSet
   */
  private static class CleanupFilesJob extends Job1<Void, GoogleCloudStorageFileSet> {
    private static final long serialVersionUID = 1386781994496334846L;
    private static final GcsService gcs = GcsServiceFactory.createGcsService();
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
      return immediate(null);
    }
  }

  @Override
  public Value<MapReduceResult<R>> run(
      MapReduceSpecification<I, K, V, O, R> mrSpec, MapReduceSettings settings) {
    getAndSaveBucketName(settings);
    String mrJobId = getJobKey().getName();
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> mapResult = futureCall(
        new MapJob<I, K, V>(mrJobId, mrSpec, settings), Job.onBackend(settings.getBackend()));
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> sortResult = futureCall(
        new SortJob(mrJobId, mrSpec, settings), mapResult, Job.onBackend(settings.getBackend()));
    futureCall(new CleanupPipelineJob(mrJobId, settings), mapResult,
        Job.onBackend(settings.getBackend()), waitFor(sortResult));
    FutureValue<MapReduceResult<R>> reduceResult = futureCall(
        new ReduceJob<K, V, O, R>(mrJobId, mrSpec, settings), mapResult, sortResult,
        Job.onBackend(settings.getBackend()));
    futureCall(new CleanupPipelineJob(mrJobId, settings), sortResult,
        Job.onBackend(settings.getBackend()), waitFor(reduceResult));
    return reduceResult;
  }

  // TODO(user): Perhaps we should have some sort of generalized settings processing.
  private static String getAndSaveBucketName(MapReduceSettings settings) {
    String bucket = settings.getBucketName();
    if (bucket == null || bucket.length() == 0) {
      try { // TODO(user): Update this once b/6009907 is resolved.
        bucket = FileServiceFactory.getFileService().getDefaultGsBucketName();
        if (bucket == null || bucket.length() == 0) {
          throw new IllegalArgumentException("A GCS bucket to write intermediate data to was not "
              + "provided in the MapReduceSettings object, and this application does not have a "
              + "default bucket configured to fall back on.");
        }
      } catch (IOException e) {
        throw new RuntimeException("A GCS bucket to write intermediate data to was not "
            + "provided in the MapReduceSettings, and could not get the default bucket.", e);
      }
      settings.setBucketName(bucket);
    }
    return bucket;
  }

}
