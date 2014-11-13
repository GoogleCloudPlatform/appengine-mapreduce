package com.google.appengine.demos.mapreduce.randomcollisions;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.appidentity.AppIdentityServiceFailureException;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.MarshallingOutput;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is an example Map Reduce that demos parallel computation.
 *
 *  <p>For the purposes of illustration this MapReduce looks for collisions in Java's Random number
 * generator. (It does not find any.)
 *
 *  <p>Here a collision is defined as multiple seed values that when next is called produce the same
 * output value.
 *
 *  <p>The input source is a range of numbers to test, and any collisions are logged and written out
 * to a file in Google Cloud Storage.
 */
@SuppressWarnings("serial")
public class CollisionFindingServlet extends HttpServlet {

  private static final Logger log = Logger.getLogger(CollisionFindingServlet.class.getName());

  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final SecureRandom random = new SecureRandom();

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String xsrfToken = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + xsrfToken, true);
    try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
      pw.println("<html><body>" + "<form method='post' > <input type='hidden' name='token' value='"
          + xsrfToken
          + "'> Run a MapReduce that looks for seeds to java's Random that result in the "
          + "same initial number being created.  <div> <br />  GCS Bucket to store data:"
          + "<input name='gcs_bucket' /> (Leave empty to use the app's default bucket) <br />"
          + "Starting integer to test: <input value='0' name='start' /> <br />"
          + "Final integer to test: <input value='3000000' name='limit' />"
          + "(Must be greater than starting integer and less than 2^31-1) <br />"
          + "Number of shards <input value='10' name='shards' />"
          + "(Must be between 1 and 100) <br />"
          + "Queue name: <input name='queue' value='mapreduce-workers'/><br />"
          + "Module: <input name='module' value='mapreduce'/><br />"
          + "<input type='submit' value='Start MapReduce' />"
          + "</div> </form> </body></html>");
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
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    String xsrfToken = req.getParameter("token");
    if (!memcache.delete(userService.getCurrentUser().getUserId() + " " + xsrfToken)) {
      throw new RuntimeException(
          "Bad XSRF token, (It probably just expired) go back, refresh the page and try again: "
          + xsrfToken);
    }

    String queue = getStringParam(req, "queue", "mapreduce-workers");
    String module = getStringParam(req, "module", "mapreduce");
    String bucket = getBucketParam(req);
    long start = getLongParam(req, "start", 0);
    long limit = getLongParam(req, "limit", 3 * 1000 * 1000);
    int shards = Math.max(1, Math.min(100, Ints.saturatedCast(getLongParam(req, "shards", 10))));
    // [START start_mapreduce]
    MapReduceSpecification<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
        mapReduceSpec = createMapReduceSpec(bucket, start, limit, shards);
    MapReduceSettings settings = getSettings(bucket, queue, module);
    // [START startMapReduceJob]
    String id = MapReduceJob.start(mapReduceSpec, settings);
    // [END startMapReduceJob]
    // [END start_mapreduce]
    resp.sendRedirect("/_ah/pipeline/status.html?root=" + id);
  }

  static String getBucketParam(HttpServletRequest req) {
    String bucket = req.getParameter("gcs_bucket");
    if (Strings.isNullOrEmpty(bucket)) {
      try {
        bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
      } catch (AppIdentityServiceFailureException ex) {
        // ignore
      }
    }
    return bucket;
  }

  // [START createMapReduceSpec]
  public static MapReduceSpecification<Long, Integer, Integer, ArrayList<Integer>,
      GoogleCloudStorageFileSet> createMapReduceSpec(String bucket, long start, long limit,
          int shards) {
    ConsecutiveLongInput input = new ConsecutiveLongInput(start, limit, shards);
    Mapper<Long, Integer, Integer> mapper = new SeedToRandomMapper();
    Marshaller<Integer> intermediateKeyMarshaller = Marshallers.getIntegerMarshaller();
    Marshaller<Integer> intermediateValueMarshaller = Marshallers.getIntegerMarshaller();
    Reducer<Integer, Integer, ArrayList<Integer>> reducer = new CollisionFindingReducer();
    Marshaller<ArrayList<Integer>> outputMarshaller = Marshallers.getSerializationMarshaller();

    Output<ArrayList<Integer>, GoogleCloudStorageFileSet> output = new MarshallingOutput<>(
        new GoogleCloudStorageFileOutput(bucket, "CollidingSeeds-%04d", "integers"),
        outputMarshaller);
    // [START mapReduceSpec]
    MapReduceSpecification<Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet>
        spec = new MapReduceSpecification.Builder<>(input, mapper, reducer, output)
            .setKeyMarshaller(intermediateKeyMarshaller)
            .setValueMarshaller(intermediateValueMarshaller)
            .setJobName("DemoMapreduce")
            .setNumReducers(shards)
            .build();
    // [END mapReduceSpec]
    return spec;
  }
  // [END createMapReduceSpec]

  // [START getSettings]
  public static MapReduceSettings getSettings(String bucket, String queue, String module) {
    // [START mapReduceSettings]
    MapReduceSettings settings = new MapReduceSettings.Builder()
        .setBucketName(bucket)
        .setWorkerQueueName(queue)
        .setModule(module) // if queue is null will use the current queue or "default" if none
        .build();
    // [END mapReduceSettings]
    return settings;
  }
  // [END getSettings]

  static String getStringParam(HttpServletRequest req, String param, String defaultValue) {
    String value = req.getParameter(param);
    return (value == null || (value = value.trim()).isEmpty()) ? defaultValue : value;
  }

  static long getLongParam(HttpServletRequest req, String param, long defaultValue) {
    String value = req.getParameter(param);
    if (value == null || (value = value.trim()).isEmpty()) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }
}
