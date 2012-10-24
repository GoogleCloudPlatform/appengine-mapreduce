package com.google.appengine.tools.mapreduce.impl.shardedjob;


/**
 * A simple intermediate tasks object to be used in unit tests. Not that this differers from
 * TestWorkerTask in that it does not use workerResult for its result.
 *
 */
public class TestTask implements IncrementalTask<TestTask, Integer> {
  private static final long serialVersionUID = 1L;
  private final int result;
  private final TestTask followupTask;

  public TestTask(int result, TestTask followupTask) {
    this.result = result;
    this.followupTask = followupTask;
  }

  @Override
  public RunResult<TestTask, Integer> run() {
    return RunResult.of(result, followupTask);
  }
}