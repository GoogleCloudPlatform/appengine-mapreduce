// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.impl.BaseContext;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.MapOnlyShardTask;
import com.google.appengine.tools.mapreduce.impl.MapReduceResultImpl;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.pipeline.ExamineStatusAndReturnResult;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A Pipeline job that runs a map jobs.
 *
 * @param <I> type of input values``
 * @param <O> type of output values
 * @param <R> type of final result
 */
public class MapJob<I, O, R> extends Job0<MapReduceResult<R>> {

  private static final long serialVersionUID = 723635736794527552L;
  private static final Logger log = Logger.getLogger(MapJob.class.getName());

  private final MapSpecification<I, O, R> specification;
  private final MapSettings settings;

  public MapJob(MapSpecification<I, O, R> specification, MapSettings settings) {
    this.specification = specification;
    this.settings = settings;
  }

  /**
   * Starts a {@link MapJob} with the given parameters in a new Pipeline.
   * Returns the pipeline id.
   */
  public static <I, O, R> String start(MapSpecification<I, O, R> specification,
      MapSettings settings) {
    if (settings.getWorkerQueueName() == null) {
      settings = new MapSettings.Builder(settings).setWorkerQueueName("default").build();
    }
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    return pipelineService.startNewPipeline(
        new MapJob<>(specification, settings), settings.toJobSettings());
  }

  static class WorkerController<I, O, R, C extends WorkerContext<O>> extends
      ShardedJobController<WorkerShardTask<I, O, C>> {

    private static final long serialVersionUID = 931651840864967980L;

    private final String mrJobId;
    private final Counters counters;
    private final Output<O, R> output;
    private final String resultPromiseHandle;

    WorkerController(String mrJobId, String shardedJobName, Counters initialCounters,
        Output<O, R> output, String resultPromiseHandle) {
      super(shardedJobName);
      this.mrJobId = checkNotNull(mrJobId, "Null jobId");
      this.counters = checkNotNull(initialCounters, "Null counters");
      this.output = checkNotNull(output, "Null output");
      this.resultPromiseHandle = checkNotNull(resultPromiseHandle, "Null resultPromiseHandle");
    }

    @Override
    public void completed(List<? extends WorkerShardTask<I, O, C>> workers) {
      ImmutableList.Builder<OutputWriter<O>> outputWriters = ImmutableList.builder();
      for (WorkerShardTask<I, O, C> worker : workers) {
        outputWriters.add(worker.getOutputWriter());
      }
      output.setContext(new BaseContext(mrJobId));
      R outputResult;
      try {
        outputResult = output.finish(outputWriters.build());
      } catch (IOException e) {
        throw new RuntimeException(output + ".finish() threw IOException");
      }
      for (WorkerShardTask<I, O, C> worker : workers) {
        counters.addAll(worker.getContext().getCounters());
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

  @Override
  public Value<MapReduceResult<R>> run() {
    MapSettings settings = this.settings;
    if (settings.getWorkerQueueName() == null) {
      String queue = getOnQueue();
      if (queue == null) {
        log.warning("workerQueueName is null and current queue is not available in the pipeline"
            + " job, using 'default'");
        queue = "default";
      }
      settings = new MapReduceSettings.Builder().setWorkerQueueName(queue).build();
    }
    String jobId = getJobKey().getName();
    Context context = new BaseContext(jobId);
    Input<I> input = specification.getInput();
    input.setContext(context);
    List<? extends InputReader<I>> readers;
    try {
      readers = input.createReaders();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Output<O, R> output = specification.getOutput();
    output.setContext(context);
    String shardedJobName = specification.getJobName() + " (map-only)";
    List<? extends OutputWriter<O>> writers = output.createWriters(readers.size());
    Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
        shardedJobName, readers.size(), writers.size());
    ImmutableList.Builder<WorkerShardTask<I, O, MapOnlyMapperContext<O>>> mapTasks =
        ImmutableList.builder();
    for (int i = 0; i < readers.size(); i++) {
      mapTasks.add(new MapOnlyShardTask<>(jobId, i, readers.size(), readers.get(i),
          specification.getMapper(), writers.get(i), settings.getMillisPerSlice()));
    }
    ShardedJobSettings shardedJobSettings = settings.toShardedJobSettings(jobId, getPipelineKey());
    PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
    WorkerController<I, O, R, MapOnlyMapperContext<O>> workerController = new WorkerController<>(
        jobId, shardedJobName, new CountersImpl(), output, resultAndStatus.getHandle());
    ShardedJob<?> shardedJob =
        new ShardedJob<>(jobId, mapTasks.build(), workerController, shardedJobSettings);
    FutureValue<Void> shardedJobResult = futureCall(shardedJob, settings.toJobSettings());
    JobSetting[] jobSetting = settings.toJobSettings(waitFor(shardedJobResult),
            statusConsoleUrl(shardedJobSettings.getMapReduceStatusUrl()), maxAttempts(1));
    return futureCall(new ExamineStatusAndReturnResult<R>(jobId), resultAndStatus, jobSetting);
  }

  /**
   * @param ex The cancellation exception
   */
  public Value<MapReduceResult<R>> handleException(CancellationException ex) {
    String mrJobId = getJobKey().getName();
    ShardedJobServiceFactory.getShardedJobService().abortJob(mrJobId);
    return null;
  }

  public Value<MapReduceResult<R>> handleException(Throwable t) throws Throwable {
    log.log(Level.SEVERE, "MapJob failed because of: ", t);
    throw t;
  }

  @Override
  public String getJobDisplayName() {
    return Optional.fromNullable(specification.getJobName()).or(super.getJobDisplayName());
  }
}
