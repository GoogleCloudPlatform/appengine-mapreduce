package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * As part of its operation, the {@code ShardedJobService} will enqueue task
 * queue tasks that send requests to the URLs specified in
 * {@link ShardedJobSettings}.  It is the user's responsibility to arrange
 * for these requests to be passed back into {@link #completeShard}
 * and {@link #runTask}.
 */
public interface ShardedJobHandler {

  public static final String JOB_ID_PARAM = "job";
  public static final String TASK_ID_PARAM = "task";
  public static final String SEQUENCE_NUMBER_PARAM = "seq";

  /**
   * Is invoked by the servlet that handles
   * {@link ShardedJobSettings#getControllerPath} when a shard has completed.
   */
  void completeShard(final String jobId, final String taskId);

  /**
   * Is invoked by the servlet that handles
   * {@link ShardedJobSettings#getWorkerPath} to run a task.
   */
  void runTask(final String jobId, final String taskId, final int sequenceNumber);
}
