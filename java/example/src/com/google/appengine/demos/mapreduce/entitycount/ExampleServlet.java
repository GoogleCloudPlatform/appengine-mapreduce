package com.google.appengine.demos.mapreduce.entitycount;

import static java.lang.Integer.parseInt;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Serves a page that allows interaction with this MapReduce demo.
 *
 * @author ohler@google.com (Christian Ohler)
 */
@SuppressWarnings("serial")
public class ExampleServlet extends HttpServlet {

  private static final Logger log = Logger.getLogger(ExampleServlet.class.getName());

  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
  private final SecureRandom random = new SecureRandom();

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String token = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + token, true);
    String bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
    try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
      pw.println("<html><body>"
          + "<br><form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "<input type='hidden' name='action' value='create'>"
          + "Run MapReduce that creates random MapReduceTest entities,"
          + " <input name='shardCount' value='1'> shards,"
          + " creating <input name='entitiesPerShard' value='1000'> entities per shard,"
          + " <input name='payloadBytesPerEntity' value='1000'> payload bytes per entity,"
          + " <input name='gcs_bucket' value='" + bucket + "'> GCS bucket:"
          + " <input type='submit' value='Make data'></form>"

          + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "<input type='hidden' name='action' value='run'>"
          + "Run MapReduce over MapReduceTest entities"
          + " with <input name='mapShardCount' value='10'> map shards"
          + " and <input name='reduceShardCount' value='2'> reduce shards,"
          + " <input name='gcs_bucket' value='" + bucket + "'> GCS bucket:"
          + " <input type='submit' value='Run'></form>"

          + "<br>"
          + "<br>"

          + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "<input type='hidden' name='action' value='viewJobResult'>"
          + "View result of job <input name='jobId'>"
          + " <input type='submit' value='View'></form>"

          + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "<input type='hidden' name='action' value='getBlob'>"
          + "Download blob with blob key or file path"
          + " <input name='keyOrFilePath'>"
          + " <input type='submit' value='Get blob'></form>"

          + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "<input type='hidden' name='action' value='deleteMapReduceBlobs'>"
          + "Delete all blobs that look like intermediate MapReduce data (based on mime type)"
          + " <input type='submit' value='Delete blobs (!)'>"

          + "</body></html>");
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

  private MapReduceSettings getSettings(String bucket) {
    return new MapReduceSettings().setWorkerQueueName("mapreduce-workers")
        .setBucketName(bucket).setModule("mapreduce");
  }

  private String startCreationJob(String bucket, int bytesPerEntity, int entitiesPerShard,
      int shardCount) {
    return MapReduceJob.start(
        MapReduceSpecification.of(
            "Create MapReduce entities",
            new ConsecutiveLongInput(0, entitiesPerShard * (long) shardCount, shardCount),
            new EntityCreator("MapReduceTest", bytesPerEntity),
            Marshallers.getVoidMarshaller(),
            Marshallers.getVoidMarshaller(),
            NoReducer.<Void, Void, Void>create(),
            NoOutput.<Void, Void>create(1)),
        getSettings(bucket));
  }

  private String startStatsJob(String bucket, int mapShardCount, int reduceShardCount) {
    return MapReduceJob.start(
        MapReduceSpecification.of(
            "MapReduceTest stats",
            new DatastoreInput("MapReduceTest", mapShardCount),
            new CountMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new CountReducer(),
            new InMemoryOutput<KeyValue<String, Long>>(reduceShardCount)),
        getSettings(bucket));
  }

  private String getUrlBase(HttpServletRequest req) throws MalformedURLException {
    URL requestUrl = new URL(req.getRequestURL().toString());
    String portString = requestUrl.getPort() == -1 ? "" : ":" + requestUrl.getPort();
    return requestUrl.getProtocol() + "://" + requestUrl.getHost() + portString + "/";
  }

  private String getPipelineStatusUrl(String urlBase, String pipelineId) {
    return urlBase + "_ah/pipeline/status.html?root=" + pipelineId;
  }

  private void redirectToPipelineStatus(HttpServletRequest req, HttpServletResponse resp,
      String pipelineId) throws IOException {
    String destinationUrl = getPipelineStatusUrl(getUrlBase(req), pipelineId);
    log.info("Redirecting to " + destinationUrl);
    resp.sendRedirect(destinationUrl);
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    String token = req.getParameter("token");
    if (memcache.get(userService.getCurrentUser().getUserId() + " " + token) == null) {
      throw new RuntimeException("Bad token, try again: " + token);
    }
    String bucket = req.getParameter("gcs_bucket");
    if (Strings.isNullOrEmpty(bucket)) {
      bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
    }
    String action = req.getParameter("action");
    if ("create".equals(action)) {
      redirectToPipelineStatus(req, resp,
          startCreationJob(bucket,
              parseInt(req.getParameter("payloadBytesPerEntity")),
              parseInt(req.getParameter("entitiesPerShard")),
              parseInt(req.getParameter("shardCount"))));
    } else if ("run".equals(action)) {
      redirectToPipelineStatus(req, resp,
          startStatsJob(bucket,
              parseInt(req.getParameter("mapShardCount")),
              parseInt(req.getParameter("reduceShardCount"))));
    } else if ("viewJobResult".equals(action)) {
      try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
        pw.println(pipelineService.getJobInfo(req.getParameter("jobId")).getOutput());
      } catch (NoSuchObjectException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Bad action: " + action);
    }
  }
}
