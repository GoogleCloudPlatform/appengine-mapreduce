package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapReduceResult;
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
public class CleanupPipelineJob extends
    Job1<Void, MapReduceResult<List<GoogleCloudStorageFileSet>>> {

  private static final long serialVersionUID = -5473046989460252781L;

  private CleanupPipelineJob() {
    // should only be called by the static cleanup method
  }

  @Override
  public Value<Void> run(MapReduceResult<List<GoogleCloudStorageFileSet>> result) {
    List<GoogleCloudStorageFileSet> files = result.getOutputResult();
    JobSetting[] settings = new JobSetting[files.size() + 3];
    settings[0] = onBackend(getOnBackend());
    settings[1] = onModule(getOnModule());
    settings[2] = onQueue(getOnQueue());
    int index = 3;
    for (GoogleCloudStorageFileSet fileSet : files) {
      FutureValue<Void> futureCall = futureCall(new DeleteFilesJob(), immediate(fileSet));
      settings[index++] = waitFor(futureCall);
    }
    // TODO(user): should not be needed once b/9940384 is fixed
    futureCall(new DeletePipelineJob(getPipelineKey().getName()), settings);
    return null;
  }

  public static void cleanup(Value<MapReduceResult<List<GoogleCloudStorageFileSet>>> mapResult,
      JobSetting... settings) {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    service.startNewPipelineUnchecked(new CleanupPipelineJob(), new Object[]{mapResult}, settings);
  }
}