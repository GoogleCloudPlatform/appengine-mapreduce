package com.google.appengine.demos.mapreduce.bigqueryload;

import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapSettings;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.bigqueryjobs.BigQueryLoadGoogleCloudStorageFilesJob;
import com.google.appengine.tools.mapreduce.bigqueryjobs.BigQueryLoadJobReference;
import com.google.appengine.tools.mapreduce.impl.BigQueryMarshallerByType;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.outputs.BigQueryGoogleCloudStorageStoreOutput;
import com.google.appengine.tools.mapreduce.outputs.BigQueryStoreResult;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.SecureRandom;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class RandomDataLoadServlet extends HttpServlet {

  private static final long serialVersionUID = 4455877588036546391L;

  private static final String BUCKET = "mapreduce-example";
  private static final String DATASET_ID = "bigquery_example";
  private static final String TABLE_NAME = "example_table";
  private static final String PROJECT_ID = "mapreduce-example";
  private static final int MAX_ROWS_PER_WRITER = 10000;


  private static final Logger log = Logger.getLogger(RandomDataLoadServlet.class.getName());
  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final SecureRandom random = new SecureRandom();

  private final int MAX_ALLOWED_ROWS = 10000;

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String token = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + token, true);

    try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
      pw.println(
          "<html><head>"
              + "<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\">"
              + "<title>Big Query data load</title></head>" + "<body>"
              + "<div> Note : This example does not work on locally devappserver.</div>"
              + "<div id=\"loadform\"><label>Loads the given number of rows(less than 10,000) into bigquery table bigquery_example:example_table.</label></div>"
              + "<form action=\"/randomDataLoad\" method=\"post\">"
              + "<label>Number of rows to load</label><input value='1000' name='row_count' />"
              + "<div><input type=\"submit\" value=\"Load\"></div>" + "</form></body></html>");
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    writeResponse(resp);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    int rowCount = Integer.parseInt(req.getParameter("row_count"));

    if (rowCount > MAX_ALLOWED_ROWS) {
      try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
        pw.println("<html><body>" + "<br> Please provide a value less than 10,0000"
            + "<br> <a href=index.html>back</a><br>" + "</body></html>");
      }
      return;
    }

    PipelineService service = PipelineServiceFactory.newPipelineService();
    redirectToPipelineStatus(resp, service.startNewPipeline(new BigQueryLoadJob(rowCount)));
  }

  private String getPipelineStatusUrl(String pipelineId) {
    return "/_ah/pipeline/status.html?root=" + pipelineId;
  }

  private void redirectToPipelineStatus(HttpServletResponse resp, String pipelineId)
      throws IOException {
    String destinationUrl = getPipelineStatusUrl(pipelineId);
    log.info("Redirecting to " + destinationUrl);
    resp.sendRedirect(destinationUrl);
  }

  static class ExtractMapReduceResult extends Job1<BigQueryStoreResult<GoogleCloudStorageFileSet>,
      MapReduceResult<BigQueryStoreResult<GoogleCloudStorageFileSet>>> {

    private static final long serialVersionUID = -342427357210338334L;

    @Override
    public Value<BigQueryStoreResult<GoogleCloudStorageFileSet>> run(
        MapReduceResult<BigQueryStoreResult<GoogleCloudStorageFileSet>> mapResult)
        throws Exception {

      BigQueryStoreResult<GoogleCloudStorageFileSet> result = mapResult.getOutputResult();
      return immediate(result);
    }

  }

  private static class BigQueryLoadJob extends Job0<List<BigQueryLoadJobReference>> {
    private final int rowCount;

    public BigQueryLoadJob(int rowCount) {
      this.rowCount = rowCount;
    }

    private static final long serialVersionUID = -8154502196962825204L;

    @Override
    public Value<List<BigQueryLoadJobReference>> run() throws Exception {
      FutureValue<MapReduceResult<BigQueryStoreResult<GoogleCloudStorageFileSet>>> bqLoadJob =
          futureCall(new MapJob<>(getInputJobSpec(rowCount), getSettings()));

      FutureValue<BigQueryStoreResult<GoogleCloudStorageFileSet>> extractJob =
          futureCall(new ExtractMapReduceResult(), bqLoadJob);
      return futureCall(
          new BigQueryLoadGoogleCloudStorageFilesJob(DATASET_ID, TABLE_NAME, PROJECT_ID),
          extractJob);
    }

    private MapSpecification<Long, SampleTable, BigQueryStoreResult<GoogleCloudStorageFileSet>> getInputJobSpec(
        int rowCount) {
      BigQueryGoogleCloudStorageStoreOutput<SampleTable> bqStore =
          new BigQueryGoogleCloudStorageStoreOutput<SampleTable>(
              new BigQueryMarshallerByType<SampleTable>(SampleTable.class), BUCKET,
              getJobKey().getName());

      int numShards = (rowCount / MAX_ROWS_PER_WRITER) + 1;
      MapSpecification<Long, SampleTable, BigQueryStoreResult<GoogleCloudStorageFileSet>> spec =
          new MapSpecification.Builder<>(new ConsecutiveLongInput(0, rowCount, numShards),
              new RandomBigQueryDataCreator(), bqStore).setJobName("Load random data into BigQuery")
              .build();
      return spec;
    }

    private MapSettings getSettings() {
      MapSettings settings = new MapSettings.Builder().setWorkerQueueName("mapreduce-workers")
          .setModule("mapreduce").build();
      return settings;
    }
  }
}
