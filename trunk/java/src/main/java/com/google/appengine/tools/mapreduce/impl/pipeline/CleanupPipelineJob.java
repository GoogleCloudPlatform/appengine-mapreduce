package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;

import java.util.List;

/**
 * A pipeline to delete MR result with a list of GoogleCloudStorageFileSet
 * and removing its traces when completed (therefore should be called
 * as a new pipeline via the {@link #cleanup} method).
 */
public class CleanupPipelineJob extends Job1<Void, List<GoogleCloudStorageFileSet>> {

  private static final long serialVersionUID = -5473046989460252781L;

  private CleanupPipelineJob() {
    // should only be called by the static cleanup method
  }

  @Override
  public Value<Void> run(List<GoogleCloudStorageFileSet> files) {
    JobSetting[] settings = new JobSetting[files.size()];
    int index = 0;
    for (GoogleCloudStorageFileSet fileSet : files) {
      FutureValue<Void> futureCall = futureCall(new DeleteFilesJob(), immediate(fileSet));
      settings[index++] = waitFor(futureCall);
    }
    // TODO(user): should not be needed once b/9940384 is fixed
    futureCall(new DeletePipelineJob(getPipelineKey().getName()), settings);
    return null;
  }

  public static void cleanup(List<GoogleCloudStorageFileSet> files, JobSetting... settings) {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    service.startNewPipeline(new CleanupPipelineJob(), files, settings);
  }
}