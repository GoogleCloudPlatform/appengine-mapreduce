// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.MapJob.WorkerController;
import com.google.appengine.tools.mapreduce.impl.BaseContext;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageMapOutput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageReduceInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortInput;
import com.google.appengine.tools.mapreduce.impl.GoogleCloudStorageSortOutput;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.MapShardTask;
import com.google.appengine.tools.mapreduce.impl.ReduceShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.pipeline.CleanupPipelineJob;
import com.google.appengine.tools.mapreduce.impl.pipeline.ExamineStatusAndReturnResult;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.sort.SortContext;
import com.google.appengine.tools.mapreduce.impl.sort.SortShardTask;
import com.google.appengine.tools.mapreduce.impl.sort.SortWorker;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
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
public class MapReduceJob<I, K, V, O, R> extends Job0<MapReduceResult<R>> {

  private static final long serialVersionUID = 723635736794527552L;
  private static final Logger log = Logger.getLogger(MapReduceJob.class.getName());

  private final MapReduceSpecification<I, K, V, O, R> specification;
  private final MapReduceSettings settings;

  public MapReduceJob(MapReduceSpecification<I, K, V, O, R> specification,
      MapReduceSettings settings) {
    this.specification = specification;
    this.settings = settings;
  }

  /**
   * Starts a {@link MapReduceJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  public static <I, K, V, O, R> String start(
      MapReduceSpecification<I, K, V, O, R> specification, MapReduceSettings settings) {
    if (settings.getWorkerQueueName() == null) {
      settings = new MapReduceSettings.Builder(settings).setWorkerQueueName("default").build();
    }
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(
        new MapReduceJob<>(specification, settings), settings.toJobSettings());
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
     * @returns A future containing the GoogleCloudStorageFileSets for all of the mappers.
     *          (Ordered by map shard number.)
     */
    @Override
    public Value<MapReduceResult<List<GoogleCloudStorageFileSet>>> run() {
      Context context = new BaseContext(mrJobId);
      Input<I> input = mrSpec.getInput();
      input.setContext(context);
      List<? extends InputReader<I>> readers;
      try {
        readers = input.createReaders();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Output<KeyValue<K, V>, List<GoogleCloudStorageFileSet>> output =
          new GoogleCloudStorageMapOutput<>(settings.getBucketName(), mrJobId,
              mrSpec.getKeyMarshaller(), mrSpec.getValueMarshaller(),
              new HashingSharder(mrSpec.getNumReducers()));
      output.setContext(context);
      String shardedJobName = mrSpec.getJobName() + " (map phase)";
      List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters(readers.size());
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> mapTasks =
          ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        mapTasks.add(new MapShardTask<>(mrJobId, i, readers.size(), readers.get(i),
            mrSpec.getMapper(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(shardedJobId, getPipelineKey());
      PromisedValue<ResultAndStatus<List<GoogleCloudStorageFileSet>>> resultAndStatus =
          newPromise();
      WorkerController<I, KeyValue<K, V>, List<GoogleCloudStorageFileSet>, MapperContext<K, V>>
          workerController = new WorkerController<>(
              mrJobId, shardedJobName, new CountersImpl(), output, resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, mapTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
      return futureCall(
          new ExamineStatusAndReturnResult<List<GoogleCloudStorageFileSet>>(shardedJobId),
          resultAndStatus, settings.toJobSettings(waitFor(shardedJobResult),
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
        for (GoogleCloudStorageFileSet fileSet : mapOutputs) {
          reduceFiles.add(fileSet.getFile(reduceShard).getObjectName());
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
      Context context = new BaseContext(mrJobId);
      int reduceShards = mrSpec.getNumReducers();
      List<GoogleCloudStorageFileSet> mapOutput =
          transposeReaders(mapResult.getOutputResult(), settings.getBucketName(), reduceShards);
      GoogleCloudStorageSortInput input = new GoogleCloudStorageSortInput(mapOutput);
      ((Input<?>) input).setContext(context);
      List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> readers = input.createReaders();
      Output<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, List<GoogleCloudStorageFileSet>>
          output = new GoogleCloudStorageSortOutput(settings.getBucketName(), mrJobId);
      output.setContext(context);
      String shardedJobName = mrSpec.getJobName() + " (sort phase)";
      List<? extends OutputWriter<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>>> writers =
          output.createWriters(reduceShards);
      Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<ByteBuffer, ByteBuffer>,
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>, SortContext>> sortTasks =
              ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        sortTasks.add(new SortShardTask(mrJobId, i, readers.size(), readers.get(i),
            new SortWorker(), writers.get(i)));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(shardedJobId, getPipelineKey());
      PromisedValue<ResultAndStatus<List<GoogleCloudStorageFileSet>>> resultAndStatus =
          newPromise();
      WorkerController<KeyValue<ByteBuffer, ByteBuffer>,
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>,
          List<GoogleCloudStorageFileSet>, SortContext> workerController = new WorkerController<>(
              mrJobId, shardedJobName, new CountersImpl(), output, resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, sortTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
      return futureCall(
          new ExamineStatusAndReturnResult<List<GoogleCloudStorageFileSet>>(shardedJobId),
          resultAndStatus, settings.toJobSettings(waitFor(shardedJobResult),
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
      Context context = new BaseContext(mrJobId);
      Output<O, R> output = mrSpec.getOutput();
      output.setContext(context);
      GoogleCloudStorageReduceInput<K, V> input = new GoogleCloudStorageReduceInput<>(
          sortResult.getOutputResult(), mrSpec.getKeyMarshaller(), mrSpec.getValueMarshaller());
      ((Input<?>) input).setContext(context);
      List<? extends InputReader<KeyValue<K, Iterator<V>>>> readers = input.createReaders();
      String shardedJobName = mrSpec.getJobName() + " (reduce phase)";
      List<? extends OutputWriter<O>> writers = output.createWriters(mrSpec.getNumReducers());
      Preconditions.checkArgument(readers.size() == writers.size(), "%s: %s readers, %s writers",
          shardedJobName, readers.size(), writers.size());
      ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>
          reduceTasks = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i++) {
        reduceTasks.add(new ReduceShardTask<>(mrJobId, i, readers.size(), readers.get(i),
            mrSpec.getReducer(), writers.get(i), settings.getMillisPerSlice()));
      }
      ShardedJobSettings shardedJobSettings =
          settings.toShardedJobSettings(shardedJobId, getPipelineKey());
      PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
      WorkerController<KeyValue<K, Iterator<V>>, O, R, ReducerContext<O>> workerController =
          new WorkerController<>(mrJobId, shardedJobName, mapResult.getCounters(), output,
              resultAndStatus.getHandle());
      ShardedJob<?> shardedJob =
          new ShardedJob<>(shardedJobId, reduceTasks.build(), workerController, shardedJobSettings);
      FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
      return futureCall(new ExamineStatusAndReturnResult<R>(shardedJobId), resultAndStatus,
          settings.toJobSettings(waitFor(shardedJobResult), maxAttempts(1),
              statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl())));
    }

    @SuppressWarnings("unused")
    public Value<MapReduceResult<R>> handleException(CancellationException ex) {
      ShardedJobServiceFactory.getShardedJobService().abortJob(shardedJobId);
      return null;
    }
  }

  private static class Cleanup extends
      Job1<Void, MapReduceResult<List<GoogleCloudStorageFileSet>>> {

    private static final long serialVersionUID = 4559443543355672948L;

    private final MapReduceSettings settings;

    public Cleanup(MapReduceSettings settings) {
      this.settings = settings;
    }

    @Override
    public Value<Void> run(MapReduceResult<List<GoogleCloudStorageFileSet>> result) {
      CleanupPipelineJob.cleanup(result.getOutputResult(), settings.toJobSettings());
      return null;
    }
  }

  @Override
  public Value<MapReduceResult<R>> run() {
    MapReduceSettings settings = this.settings;
    if (settings.getWorkerQueueName() == null) {
      String queue = getOnQueue();
      if (queue == null) {
        log.warning("workerQueueName is null and current queue is not available in the pipeline"
            + " job, using 'default'");
        queue = "default";
      }
      settings = new MapReduceSettings.Builder(settings).setWorkerQueueName(queue).build();
    }
    String mrJobId = getJobKey().getName();
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> mapResult = futureCall(
        new MapJob<>(mrJobId, specification, settings), settings.toJobSettings(maxAttempts(1)));
    FutureValue<MapReduceResult<List<GoogleCloudStorageFileSet>>> sortResult = futureCall(
        new SortJob(mrJobId, specification, settings), mapResult,
        settings.toJobSettings(maxAttempts(1)));
    futureCall(new Cleanup(settings), mapResult, waitFor(sortResult));
    FutureValue<MapReduceResult<R>> reduceResult = futureCall(
        new ReduceJob<>(mrJobId, specification, settings), mapResult, sortResult,
        settings.toJobSettings(maxAttempts(1)));
    futureCall(new Cleanup(settings), sortResult, waitFor(reduceResult));
    return reduceResult;
  }

  public Value<MapReduceResult<R>> handleException(Throwable t) throws Throwable {
    log.log(Level.SEVERE, "MapReduce job failed because of: ", t);
    throw t;
  }

  @Override
  public String getJobDisplayName() {
    return Optional.fromNullable(specification.getJobName()).or(super.getJobDisplayName());
  }
}
