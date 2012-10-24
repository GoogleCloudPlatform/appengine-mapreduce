// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.tools.mapreduce.impl.AbstractWorkerController;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.DeleteFilesJob;
import com.google.appengine.tools.mapreduce.impl.IntermediateOutput;
import com.google.appengine.tools.mapreduce.impl.MapShardTask;
import com.google.appengine.tools.mapreduce.impl.ReduceShardTask;
import com.google.appengine.tools.mapreduce.impl.ResultAndCounters;
import com.google.appengine.tools.mapreduce.impl.ShuffleJob;
import com.google.appengine.tools.mapreduce.impl.ShuffleResult;
import com.google.appengine.tools.mapreduce.impl.Util;
import com.google.appengine.tools.mapreduce.impl.WorkerResult;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.FutureValue;
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
import java.util.List;
import java.util.Map;
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
  public static <I, K, V, O, R> String start(MapReduceSpecification<I, K, V, O, R> specification,
      MapReduceSettings settings) {
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(new MapReduceJob<I, K, V, O, R>(),
        specification, settings, Util.jobSettings(settings));
  }

  public MapReduceJob() {
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "()";
  }

  private static ShardedJobSettings makeShardedJobSettings(MapReduceSettings mrSettings) {
    return new ShardedJobSettings()
        .setControllerPath(mrSettings.getBaseUrl() + MapReduceServletImpl.CONTROLLER_PATH)
        .setWorkerPath(mrSettings.getBaseUrl() + MapReduceServletImpl.WORKER_PATH)
        .setControllerBackend(mrSettings.getBackend())
        .setWorkerBackend(mrSettings.getBackend())
        .setControllerQueueName(mrSettings.getControllerQueueName())
        .setWorkerQueueName(mrSettings.getWorkerQueueName());
  }

  private static class FillPromiseJob extends Job2<Void, String, Object> {
    private static final long serialVersionUID = 850701484460334898L;

    FillPromiseJob() {
    }

    @Override public Value<Void> run(String promiseHandle, Object value) {
      try {
        PipelineServiceFactory.newPipelineService()
            .submitPromisedValue(promiseHandle, value);
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

    private final CountersImpl initialCounters;
    private final Output<O, R> output;
    private final String resultPromiseHandle;

    WorkerController(String shardedJobName, CountersImpl initialCounters,
        Output<O, R> output,
        String resultPromiseHandle) {
      super(shardedJobName);
      this.initialCounters = checkNotNull(initialCounters, "Null initialCounters");
      this.output = checkNotNull(output, "Null output");
      this.resultPromiseHandle = checkNotNull(resultPromiseHandle, "Null resultPromiseHandle");
    }

    @Override public void completed(WorkerResult<O> finalCombinedResult) {
      Map<Integer, OutputWriter<O>> closedWriterMap = finalCombinedResult.getClosedWriters();
      ImmutableList.Builder<OutputWriter<O>> closedWriters = ImmutableList.builder();
      for (int i = 0; i < closedWriterMap.size(); i++) {
        Preconditions.checkState(closedWriterMap.containsKey(i), "%s: Missing closed writer %s: %s",
            this, i, closedWriterMap);
        closedWriters.add(closedWriterMap.get(i));
      }
      R outputResult;
      try {
        outputResult = output.finish(closedWriters.build());
      } catch (IOException e) {
        throw new RuntimeException(output + ".finish() threw IOException");
      }
      CountersImpl totalCounters = new CountersImpl();
      totalCounters.addAll(initialCounters);
      totalCounters.addAll(finalCombinedResult.getCounters());
      ResultAndCounters<R> result = new ResultAndCounters<R>(outputResult, totalCounters);
      // HACK(ohler): It seems that pipeline doesn't always allow a promise to
      // be filled in the job that created the promise.  We attempt that in the
      // case where there are 0 shards, like in one of our test cases.  Or maybe
      // it's our lack of retries on NoSuchObjectException.  Need to investigate
      // further.
      PipelineServiceFactory.newPipelineService().startNewPipeline(new FillPromiseJob(),
          resultPromiseHandle, result);
    }
  }

  private interface TaskCreator<I, O, C extends WorkerContext> {
    WorkerShardTask<I, O, C> createTask(int shard, int shardCount,
        InputReader<I> input, OutputWriter<O> output);
  }

  private static <I, O, R, C extends WorkerContext> void startShardedJob(
      String shardedJobName, String shardedJobId, CountersImpl initialCounters,
      List<? extends InputReader<I>> readers,
      Output<O, R> output,
      List<? extends OutputWriter<O>> writers,
      TaskCreator<I, O, C> taskCreator,
      String resultPromiseHandle,
      MapReduceSettings settings) {
    Preconditions.checkArgument(readers.size() == writers.size(), "%s: %s readers, %s writers",
        shardedJobName, readers.size(), writers.size());
    ImmutableList.Builder<WorkerShardTask<I, O, C>> initialTasks = ImmutableList.builder();
    for (int i = 0; i < readers.size(); i++) {
      initialTasks.add(taskCreator.createTask(i, readers.size(), readers.get(i), writers.get(i)));
    }
    ShardedJobServiceFactory.getShardedJobService().startJob(
        shardedJobId,
        initialTasks.build(),
        new WorkerController<I, O, R, C>(
            shardedJobName, initialCounters, output, resultPromiseHandle),
        makeShardedJobSettings(settings));
  }

  private static class MapJob<I, K, V> extends Job0<ResultAndCounters<List<AppEngineFile>>> {
    private static final long serialVersionUID = 274712180795282822L;

    private final String mrJobId;
    private final MapReduceSpecification<I, K, V, ?, ?> mrSpec;
    private final MapReduceSettings settings;

    private MapJob(String mrJobId,
        MapReduceSpecification<I, K, V, ?, ?> mrSpec,
        MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override public Value<ResultAndCounters<List<AppEngineFile>>> run() {
      @SuppressWarnings("unchecked")
      PromisedValue<ResultAndCounters<List<AppEngineFile>>> result =
          (PromisedValue) newPromise(ResultAndCounters.class);
      String shardedJobId = mrJobId + "-map";
      List<? extends InputReader<I>> readers = Util.createReaders(mrSpec.getInput());
      Output<KeyValue<K, V>, List<AppEngineFile>> output = new IntermediateOutput<K, V>(
          mrJobId,
          readers.size(),
          mrSpec.getIntermediateKeyMarshaller(),
          mrSpec.getIntermediateValueMarshaller());
      startShardedJob(mrSpec.getJobName() + " (map phase)",
          shardedJobId,
          new CountersImpl(),
          readers, output, Util.createWriters(output),
          new TaskCreator<I, KeyValue<K, V>, MapperContext<K, V>>() {
            @Override public WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> createTask(
                int shard, int shardCount,
                InputReader<I> reader, OutputWriter<KeyValue<K, V>> writer) {
              return new MapShardTask<I, K, V>(mrJobId, shard, shardCount,
                  reader, mrSpec.getMapper(), writer,
                  settings.getMillisPerSlice());
            }
          },
          result.getHandle(),
          settings);
      setStatusConsoleUrl(settings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId);
      return result;
    }
  }

  private static class ReduceJob<K, V, O, R> extends Job2<
      MapReduceResult<R>, ResultAndCounters<List<AppEngineFile>>, ShuffleResult<K, V, O>> {
    private static final long serialVersionUID = 590237832617368335L;

    private final String mrJobId;
    private final MapReduceSpecification<?, K, V, O, R> mrSpec;
    private final MapReduceSettings settings;

    private ReduceJob(String mrJobId,
        MapReduceSpecification<?, K, V, O, R> mrSpec,
        MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
      this.settings = checkNotNull(settings, "Null settings");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override public Value<MapReduceResult<R>> run(
        ResultAndCounters<List<AppEngineFile>> mapResult,
        ShuffleResult<K, V, O> shuffleResult) {
      @SuppressWarnings("unchecked")
      PromisedValue<MapReduceResult<R>> result = (PromisedValue) newPromise(MapReduceResult.class);
      String shardedJobId = mrJobId + "-reduce";
      startShardedJob(mrSpec.getJobName() + " (reduce phase)",
          shardedJobId,
          mapResult.getCounters(),
          shuffleResult.getReducerReaders(),
          mrSpec.getOutput(),
          shuffleResult.getReducerWriters(),
          new TaskCreator<KeyValue<K, ReducerInput<V>>, O, ReducerContext<O>>() {
            @Override public WorkerShardTask<KeyValue<K, ReducerInput<V>>, O, ReducerContext<O>>
                createTask(int shard, int shardCount,
                    InputReader<KeyValue<K, ReducerInput<V>>> reader, OutputWriter<O> writer) {
              return new ReduceShardTask<K, V, O>(mrJobId, shard, shardCount,
                  reader, mrSpec.getReducer(), writer,
                  settings.getMillisPerSlice());
            }
          },
          result.getHandle(),
          settings);
      setStatusConsoleUrl(settings.getBaseUrl() + "detail?mapreduce_id=" + shardedJobId);
      return result;
    }
  }

  private static class IntermediateCleanupJob
      extends Job1<Void, ResultAndCounters<List<AppEngineFile>>> {
    private static final long serialVersionUID = 354137030664235135L;

    private final String mrJobId;
    private final MapReduceSettings settings;

    private IntermediateCleanupJob(String mrJobId,
        MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.settings = checkNotNull(settings, "Null settings");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override public Value<Void> run(ResultAndCounters<List<AppEngineFile>> mapResult) {
      futureCall(new DeleteFilesJob("" + this), immediate(mapResult.getOutputResult()),
          Util.jobSettings(settings));
      return immediate(null);
    }
  }

  private static class FinalCleanupJob<K, V, O> extends Job1<Void, ShuffleResult<K, V, O>> {
    private static final long serialVersionUID = 121832907494231026L;

    private final String mrJobId;
    private final MapReduceSettings settings;

    private FinalCleanupJob(String mrJobId,
        MapReduceSettings settings) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
      this.settings = checkNotNull(settings, "Null settings");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override public Value<Void> run(ShuffleResult<K, V, O> shuffleResult) {
      futureCall(new DeleteFilesJob("" + this),
          immediate(shuffleResult.getReducerInputFiles()),
          Util.jobSettings(settings));
      return immediate(null);
    }
  }

  public Value<MapReduceResult<R>> run(MapReduceSpecification<I, K, V, O, R> mrSpec,
      MapReduceSettings settings) {
    String mrJobId = getJobKey().getName();
    FutureValue<ResultAndCounters<List<AppEngineFile>>> mapResult = futureCall(
        new MapJob<I, K, V>(mrJobId, mrSpec, settings), Util.jobSettings(settings));
    FutureValue<ShuffleResult<K, V, O>> shuffleResult = futureCall(
        new ShuffleJob<K, V, O>(mrJobId, mrSpec, settings), mapResult, Util.jobSettings(settings));
    futureCall(new IntermediateCleanupJob(mrJobId, settings), mapResult,
        Util.jobSettings(settings, waitFor(shuffleResult)));
    FutureValue<MapReduceResult<R>> reduceResult = futureCall(
        new ReduceJob<K, V, O, R>(mrJobId, mrSpec, settings),
        mapResult, shuffleResult, Util.jobSettings(settings));
    futureCall(new FinalCleanupJob<K, V, O>(mrJobId, settings), shuffleResult,
        Util.jobSettings(settings, waitFor(reduceResult)));
    return reduceResult;
  }

}
