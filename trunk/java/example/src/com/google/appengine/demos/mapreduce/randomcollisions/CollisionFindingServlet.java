package com.google.appengine.demos.mapreduce.randomcollisions;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
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
import java.util.ArrayList;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is an example Map Reduce that demos parallel computation.
 *
 *  For the purposes of illustration this MapReduce looks for collisions in Java's Random number
 * generator. (It does not find any.)
 *
 *  Here a collision is defined as multiple seed values that when next is called produce the same
 * output value.
 *
 * The input source is a range of numbers to test, and any collisions are logged and written out to
 * a file in Google Cloud Storage.
 */
@SuppressWarnings("serial")
public class CollisionFindingServlet extends HttpServlet {

  static MapReduceSpecification<
      Long, Integer, Integer, ArrayList<Integer>, GoogleCloudStorageFileSet> createMapReduceSpec(
          String bucket, long start, long limit, int shards) {
    ConsecutiveLongInput input = new ConsecutiveLongInput(start, limit, shards);
    Mapper<Long, Integer, Integer> mapper = new SeedToRandomMapper();
    Marshaller<Integer> intermediateKeyMarshaller = Marshallers.getIntegerMarshaller();
    Marshaller<Integer> intermediateValueMarshaller = Marshallers.getIntegerMarshaller();
    Reducer<Integer, Integer, ArrayList<Integer>> reducer = new CollisionFindingReducer();
    Marshaller<ArrayList<Integer>> outputMarshaller = Marshallers.getSerializationMarshaller();

    Output<ArrayList<Integer>, GoogleCloudStorageFileSet> output = new MarshallingOutput<>(
        new GoogleCloudStorageFileOutput(bucket, "CollidingSeeds-%04d", "integers", shards),
        outputMarshaller);
    return MapReduceSpecification.of("DemoMapreduce", input, mapper, intermediateKeyMarshaller,
        intermediateValueMarshaller, reducer, output);
  }

  static MapReduceSettings getSettings(String bucket) {
    return new MapReduceSettings().setWorkerQueueName("mapreduce-workers")
        .setBucketName(bucket).setModule("mapreduce");
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String bucket = getBucketParam(req);
    long start = getLongParam(req, "start", 0);
    long limit = getLongParam(req, "limit", 100 * 1000 * 1000);
    int shards = Math.max(1, Math.min(100, Ints.saturatedCast(getLongParam(req, "shards", 30))));
    String id = MapReduceJob.start(
        createMapReduceSpec(bucket, start, limit, shards), getSettings(bucket));
    resp.sendRedirect("/_ah/pipeline/status.html?root=" + id);
  }

  static String getBucketParam(HttpServletRequest req) {
    String bucket = req.getParameter("gcs_bucket");
    if (Strings.isNullOrEmpty(bucket)) {
      bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
    }
    return bucket;
  }

  static long getLongParam(HttpServletRequest req, String param, long defaultValue) {
    String value = req.getParameter(param);
    if (value == null || value.isEmpty()) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }
}
