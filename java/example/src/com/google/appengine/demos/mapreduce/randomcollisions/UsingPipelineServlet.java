package com.google.appengine.demos.mapreduce.randomcollisions;

import static com.google.appengine.demos.mapreduce.randomcollisions.CollisionFindingServlet.createMapReduceSpec;
import static com.google.appengine.demos.mapreduce.randomcollisions.CollisionFindingServlet.getBucketParam;
import static com.google.appengine.demos.mapreduce.randomcollisions.CollisionFindingServlet.getLongParam;
import static com.google.appengine.demos.mapreduce.randomcollisions.CollisionFindingServlet.getSettings;
import static com.google.appengine.demos.mapreduce.randomcollisions.CollisionFindingServlet.getStringParam;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is an alternative to {@link CollisionFindingServlet} that uses Pipelines to start the
 * MapReduce.
 *
 * This example shows running a follow up task after the MapReduce has run.
 */
public class UsingPipelineServlet extends HttpServlet {
  private static final long serialVersionUID = -8159877366348539461L;

  private static final Logger LOG = Logger.getLogger(UsingPipelineServlet.class.getName());

  /**
   * A two step pipeline: 1. Run a MapReduce 2. Run LogFileNamesJob with the output of the
   * MapReduce.
   */
  private static class MyPipelineJob extends Job0<Void> {
    private static final long serialVersionUID = 1954542676168374323L;

    private final String bucket;
    private final long start;
    private final long limit;
    private final int shards;

    MyPipelineJob(String bucket, long start, long limit, int shards) {
      this.bucket = bucket;
      this.start = start;
      this.limit = limit;
      this.shards = shards;
    }

    @Override
    public Value<Void> run() throws Exception {
      MapReduceSpecification<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
      spec = createMapReduceSpec(bucket, start, limit, shards);
      MapReduceSettings settings = getSettings(bucket, null, null);
      // [START start_as_pipeline]
      MapReduceJob<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
          mapReduceJob = new MapReduceJob<>();
      FutureValue<MapReduceResult<GoogleCloudStorageFileSet>> mapReduceResult =
          futureCall(mapReduceJob, immediate(spec), immediate(settings));
      // [END start_as_pipeline]
      return futureCall(new LogFileNamesJob(), mapReduceResult);
    }
  }

  private static class LogFileNamesJob extends
      Job1<Void, MapReduceResult<GoogleCloudStorageFileSet>> {
    private static final long serialVersionUID = 6277239748168293296L;

    @Override
    public Value<Void> run(MapReduceResult<GoogleCloudStorageFileSet> result) {
      for (GcsFilename name : result.getOutputResult().getAllFiles()) {
        LOG.info("Output stored to file: " + name);
      }
      return null;
    }
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String queue = getStringParam(req, "queue", "mapreduce-workers");
    String module = getStringParam(req, "module", "mapreduce");
    String bucket = getBucketParam(req);
    long start = getLongParam(req, "start", 0);
    long limit = getLongParam(req, "limit", 100 * 1000 * 1000);
    int shards = Math.max(1, Math.min(100, Ints.saturatedCast(getLongParam(req, "shards", 30))));
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new MyPipelineJob(bucket, start, limit, shards),
        new JobSetting.OnQueue(queue), new JobSetting.OnModule(module));
    resp.sendRedirect("/_ah/pipeline/status.html?root=" + pipelineId);
  }
}
