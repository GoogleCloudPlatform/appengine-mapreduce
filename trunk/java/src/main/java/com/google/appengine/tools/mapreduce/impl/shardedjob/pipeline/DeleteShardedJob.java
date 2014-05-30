package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.pipeline.Job;

/**
 * A pipeline job to delete persistent data for a sharded job.
 */
public class DeleteShardedJob extends AbstractShardedJob {

  private static final long serialVersionUID = -6850669259843382958L;

  public DeleteShardedJob(String jobId, int taskCount) {
    super(jobId, taskCount);
  }

  @Override
  protected Job<?> createShardsJob(int start, int end) {
    return new DeleteShardsInfos(getJobId(), start, end);
  }

  @Override
  public String getJobDisplayName() {
    return "DeleteShardedJob: " + getJobId();
  }
}
