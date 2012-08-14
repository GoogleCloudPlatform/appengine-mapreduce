// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.blobstore.BlobInfo;
import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.ShuffleServiceFactory;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.List;
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
public class Servlet extends HttpServlet {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(Servlet.class.getName());

  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
  private final SecureRandom random = new SecureRandom();

  //private static final boolean USE_BACKENDS = true;
  private static final boolean USE_BACKENDS = false;

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String token = "" + (random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + token, true);
    PrintWriter pw = new PrintWriter(resp.getOutputStream());
    pw.println("<html><body>"
        + "<br><form method='post'><input type='hidden' name='token' value='" + token + "'>"
        + "<input type='hidden' name='action' value='create'>"
        + "Run MapReduce that creates random MapReduceTest entities,"
        + " <input name='shardCount' value='1'> shards,"
        + " creating <input name='entitiesPerShard' value='1000'> entities per shard,"
        + " <input name='payloadBytesPerEntity' value='1000'> payload bytes per entity:"
        + " <input type='submit' value='Make data'></form>"

        + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
        + "<input type='hidden' name='action' value='run'>"
        + "Run MapReduce over MapReduceTest entities"
        + " with <input name='mapShardCount' value='10'> map shards"
        + " and <input name='reduceShardCount' value='2'> reduce shards:"
        + " <input type='submit' value='Run'></form>"

        + "<br>"
        + "<br>"

        + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
        + "<input type='hidden' name='action' value='viewJobResult'>"
        + "View result of job <input name='jobId'>"
        + " <input type='submit' value='View'></form>"

        + "<form method='post'><input type='hidden' name='token' value='" + token + "'>"
        + "<input type='hidden' name='action' value='viewShuffleStatus'>"
        + "View shuffle status of shuffle job"
        + " <input name='shuffleJobId'>"
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
    pw.close();
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (userService.getCurrentUser() == null) {
      log.info("no user");
      return;
    }
    writeResponse(resp);
  }

  private MapReduceSettings getSettings() {
    MapReduceSettings settings = new MapReduceSettings()
        .setWorkerQueueName("mapreduce-workers")
        .setControllerQueueName("default");
    if (USE_BACKENDS) {
      settings.setBackend("worker");
    }
    return settings;
  }

  private String startCreationJob(int bytesPerEntity, int entitiesPerShard, int shardCount) {
    return MapReduceJob.start(
        MapReduceSpecification.of(
            "Create MapReduce entities",
            new ConsecutiveLongInput(0, entitiesPerShard * (long) shardCount, shardCount),
            new EntityCreator("MapReduceTest", bytesPerEntity),
            Marshallers.getVoidMarshaller(),
            Marshallers.getVoidMarshaller(),
            NoReducer.<Void, Void, Void>create(),
            NoOutput.<Void, Void>create(1)),
        getSettings());
  }

  private String startStatsJob(int mapShardCount, int reduceShardCount) {
    return MapReduceJob.start(
        MapReduceSpecification.of(
            "MapReduceTest stats",
            new DatastoreInput("MapReduceTest", mapShardCount),
            new CountMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new CountReducer(),
            new InMemoryOutput<KeyValue<String, Long>>(reduceShardCount)),
        getSettings());
  }

  private static class IsIntermediateMapReduceFile implements Predicate<BlobInfo>, Serializable {
    private static final long serialVersionUID = 507549464779192087L;

    private static final List<String> MAPREDUCE_INTERMEDIATE_MIME_TYPES = ImmutableList.of(
        MapReduceConstants.MAP_OUTPUT_MIME_TYPE,
        MapReduceConstants.REDUCE_INPUT_MIME_TYPE);

    @Override public boolean apply(BlobInfo info) {
      return MAPREDUCE_INTERMEDIATE_MIME_TYPES.contains(info.getContentType());
    }
  }

  private String deleteMapReduceBlobs() {
    log.info("Deleting all blobs");
    return DeleteMatchingBlobs.start(new IsIntermediateMapReduceFile());
  }

  private BlobKey findBlob(String keyOrFilePath) {
    BlobKey candidateKey;
    if (keyOrFilePath.startsWith("/blobstore/")) {
      candidateKey = FileServiceFactory.getFileService()
          .getBlobKey(new AppEngineFile(keyOrFilePath));
    } else {
      candidateKey = new BlobKey(keyOrFilePath);
    }
    if (new BlobInfoFactory().loadBlobInfo(candidateKey) != null) {
      return candidateKey;
    } else {
      throw new IllegalArgumentException(
          "Blob " + candidateKey + " does not exist: " + keyOrFilePath);
    }
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
    String action = req.getParameter("action");
    if ("create".equals(action)) {
      redirectToPipelineStatus(req, resp,
          startCreationJob(
              Integer.parseInt(req.getParameter("payloadBytesPerEntity")),
              Integer.parseInt(req.getParameter("entitiesPerShard")),
              Integer.parseInt(req.getParameter("shardCount"))));
    } else if ("run".equals(action)) {
      redirectToPipelineStatus(req, resp,
          startStatsJob(
              Integer.parseInt(req.getParameter("mapShardCount")),
              Integer.parseInt(req.getParameter("reduceShardCount"))));
    } else if ("viewJobResult".equals(action)) {
      PrintWriter pw = new PrintWriter(resp.getOutputStream());
      try {
        pw.println("" + pipelineService.getJobInfo(req.getParameter("jobId")).getOutput());
      } catch (NoSuchObjectException e) {
        throw new RuntimeException(e);
      }
      pw.close();
    } else if ("viewShuffleStatus".equals(action)) {
      PrintWriter pw = new PrintWriter(resp.getOutputStream());
      pw.println("" + ShuffleServiceFactory.getShuffleService()
          .getStatus(req.getParameter("shuffleJobId")));
      pw.close();
    } else if ("getBlob".equals(action)) {
      BlobKey blobKey = findBlob(req.getParameter("keyOrFilePath"));
      if (blobKey != null) {
        BlobstoreServiceFactory.getBlobstoreService().serve(blobKey, resp);
      } else {
        throw new RuntimeException("Blob not found");
      }
    } else if ("deleteMapReduceBlobs".equals(action)) {
      redirectToPipelineStatus(req, resp,
          deleteMapReduceBlobs());
    } else {
      throw new RuntimeException("Bad action: " + action);
    }
  }

}
