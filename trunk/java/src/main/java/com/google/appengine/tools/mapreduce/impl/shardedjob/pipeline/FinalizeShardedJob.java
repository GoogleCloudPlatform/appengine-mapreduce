package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.Job;

/**
 * A pipeline job for finalizing the job and cleaning up unnecessary state.
 */
public class FinalizeShardedJob extends AbstractShardedJob {

  private static final long serialVersionUID = -6850669259843382958L;
  private final Status status;

  public FinalizeShardedJob(String jobId, int taskCount, Status status) {
    super(jobId, taskCount);
    this.status = status;
  }

  @Override
  protected Job<?> createShardsJob(int start, int end) {
    return new FinalizeShardsInfos(getJobId(), status, start, end);
  }

  @Override
  public String getJobDisplayName() {
    return "FinalizeShardedJob: " + getJobId();
  }
}
