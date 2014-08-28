// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.tools.mapreduce.impl.BaseContext;
import com.google.appengine.tools.mapreduce.impl.CountersImpl;
import com.google.appengine.tools.mapreduce.impl.MapOnlyShardTask;
import com.google.appengine.tools.mapreduce.impl.WorkerController;
import com.google.appengine.tools.mapreduce.impl.WorkerShardTask;
import com.google.appengine.tools.mapreduce.impl.pipeline.ExamineStatusAndReturnResult;
import com.google.appengine.tools.mapreduce.impl.pipeline.ResultAndStatus;
import com.google.appengine.tools.mapreduce.impl.pipeline.ShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
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
    List<? extends OutputWriter<O>> writers = output.createWriters(readers.size());
    Preconditions.checkState(readers.size() == writers.size(), "%s: %s readers, %s writers",
        jobId, readers.size(), writers.size());
    ImmutableList.Builder<WorkerShardTask<I, O, MapOnlyMapperContext<O>>> mapTasks =
        ImmutableList.builder();
    for (int i = 0; i < readers.size(); i++) {
      mapTasks.add(new MapOnlyShardTask<>(jobId, i, readers.size(), readers.get(i),
          specification.getMapper(), writers.get(i), settings.getMillisPerSlice()));
    }
    ShardedJobSettings shardedJobSettings = settings.toShardedJobSettings(jobId, getPipelineKey());
    PromisedValue<ResultAndStatus<R>> resultAndStatus = newPromise();
    WorkerController<I, O, R, MapOnlyMapperContext<O>> workerController = new WorkerController<>(
        jobId, new CountersImpl(), output, resultAndStatus.getHandle());
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
