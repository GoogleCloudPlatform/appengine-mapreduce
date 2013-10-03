package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.common.collect.ImmutableMap;

import java.util.Collections;

/**
 * A simple task for use in unit tests. Outputs a {@link WorkerResult} so that the output can be
 * serialized and read from the store like a real task. For a simpler class use TestTask.
 *
 */
public class TestWorkerTask implements IncrementalTask<TestWorkerTask, WorkerResult<Integer>> {
  private static final long serialVersionUID = 1L;
  private final TestWorkerTask followupTask;
  private int result;
  private int shardId;

  public TestWorkerTask(int shardId, int result, TestWorkerTask followupTask) {
    this.shardId = shardId;
    this.result = result;
    this.followupTask = followupTask;
  }

  @Override
  public RunResult<TestWorkerTask, WorkerResult<Integer>> run() {
    Counters countersImpl = new CountersImpl();
    countersImpl.getCounter("TestWorkerTaskSum").increment(result);
    WorkerResult<Integer> workerResult = new WorkerResult<Integer>(
        Collections.<Integer, OutputWriter<Integer>>emptyMap(),
        ImmutableMap.<Integer, WorkerShardState>of(
            shardId, new WorkerShardState(0, System.currentTimeMillis(), "" + result)),
        countersImpl);
    return RunResult.of(workerResult, followupTask);
  }
}