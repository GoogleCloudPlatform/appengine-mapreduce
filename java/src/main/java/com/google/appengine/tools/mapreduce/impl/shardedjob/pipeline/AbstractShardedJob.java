package com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline;

import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.Jobs;
import com.google.appengine.tools.pipeline.Value;

/**
 * A base class for a sharded-job pipeline.
 */
public abstract class AbstractShardedJob extends Job0<Void> {

  private static final long serialVersionUID = 6498588928999409114L;
  private static final int SHARDS_PER_JOB = 20;
  private static final JobSetting[] CHILD_JOB_PARAMS = {};
  private final String jobId;
  private final int taskCount;

  public AbstractShardedJob(String jobId, int taskCount) {
    this.jobId = jobId;
    this.taskCount = taskCount;
  }

  @Override
  public Value<Void> run() {
    int childJobs = (int) Math.ceil(taskCount / (double) SHARDS_PER_JOB);
    FutureValue<?>[] waitFor = new FutureValue[childJobs];
    int startOffset = 0;
    for (int i = 0; i < childJobs; i++) {
      int endOffset = Math.min(taskCount, startOffset + SHARDS_PER_JOB);
      waitFor[i] = futureCallUnchecked(
          getChildJobParams(), createShardsJob(startOffset, endOffset));
      startOffset = endOffset;
    }
    return Jobs.waitForAllAndDelete(this, null, waitFor);
  }

  protected abstract Job<?> createShardsJob(int start, int end);

  protected String getJobId() {
    return jobId;
  }

  protected JobSetting[] getChildJobParams() {
    return CHILD_JOB_PARAMS;
  }
}
