package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.impl.FilesByShard;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;

/**
 * A pipeline to delete MR result with a FilesByShard and removing its traces when completed
 * (therefore should be called as a new pipeline via the {@link #cleanup} method).
 */
public class CleanupPipelineJob extends Job1<Void, FilesByShard> {

  private static final long serialVersionUID = -5473046989460252781L;

  private CleanupPipelineJob() {
    // should only be called by the static cleanup method
  }

  @Override
  public Value<Void> run(FilesByShard files) {
    JobSetting[] settings = new JobSetting[files.getShardCount()];
    int index = 0;
    for (int shard = 0; shard < files.getShardCount(); shard++) {
      FutureValue<Void> futureCall =
          futureCall(new DeleteFilesJob(), immediate(files.getFilesForShard(shard)));
      settings[index++] = waitFor(futureCall);
    }
    // TODO(user): should not be needed once b/9940384 is fixed
    return futureCall(new DeletePipelineJob(getPipelineKey().getName()), settings);
  }

  public static void cleanup(FilesByShard files, JobSetting... settings) {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    service.startNewPipeline(new CleanupPipelineJob(), files, settings);
  }
}
