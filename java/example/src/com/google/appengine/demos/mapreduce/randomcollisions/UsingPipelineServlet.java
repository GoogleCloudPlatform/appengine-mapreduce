package com.google.appengine.demos.mapreduce.randomcollisions;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;

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

    @Override
    public Value<Void> run() throws Exception {
      MapReduceJob<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
          mapReduceJob = new MapReduceJob<>();
      MapReduceSpecification<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
          spec = CollisionFindingServlet.createMapReduceSpec();
      MapReduceSettings settings = CollisionFindingServlet.getSettings();

      FutureValue<MapReduceResult<GoogleCloudStorageFileSet>> mapReduceResult =
          futureCall(mapReduceJob, immediate(spec), immediate(settings));
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
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    PipelineService service = PipelineServiceFactory.newPipelineService();
    String pipelineId = service.startNewPipeline(new MyPipelineJob());
    resp.sendRedirect("/_ah/pipeline/status.html?root=" + pipelineId);
  }

}
