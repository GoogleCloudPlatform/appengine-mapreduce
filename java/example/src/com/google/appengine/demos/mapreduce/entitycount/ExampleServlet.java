package com.google.appengine.demos.mapreduce.entitycount;

import static java.lang.Integer.parseInt;

import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.PrintWriter;
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

  private static final String DATASTORE_TYPE = "MR_EntityCount_Demo_Kind";

  private static final Logger log = Logger.getLogger(ExampleServlet.class.getName());

  private final MemcacheService memcache = MemcacheServiceFactory.getMemcacheService();
  private final UserService userService = UserServiceFactory.getUserService();
  private final SecureRandom random = new SecureRandom();

  private void writeResponse(HttpServletResponse resp) throws IOException {
    String token = String.valueOf(random.nextLong() & Long.MAX_VALUE);
    memcache.put(userService.getCurrentUser().getUserId() + " " + token, true);

    try (PrintWriter pw = new PrintWriter(resp.getOutputStream())) {
      pw.println("<html><body>"
          + "<br><form method='post'><input type='hidden' name='token' value='" + token + "'>"
          + "Runs three MapReduces: <br /> <ul> <li> Creates random MapReduceTest "
          + "entities of the type:  " + DATASTORE_TYPE +". </li> "
          + "<li> Counts the number of each character in all entities of this type. </li>"
          + "<li> Deletes all entities of the type: " + DATASTORE_TYPE + "</li> </ul> <div> <br />"
          + "Entities to create: <input name='entities' value='10000'> <br />"
          + "Entity payload size: <input name='payloadBytesPerEntity' value='1000'> <br />"
          + "ShardCount: <input name='shardCount' value='10'> <br />"
          + "GCS bucket: <input name='gcs_bucket'> (Leave empty to use the app's default bucket)"
          + "<br /> <input type='submit' value='Create, Count, and Delete'>"
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

  private String getPipelineStatusUrl(String pipelineId) {
    return "/_ah/pipeline/status.html?root=" + pipelineId;
  }

  private void redirectToPipelineStatus(HttpServletResponse resp,
      String pipelineId) throws IOException {
    String destinationUrl = getPipelineStatusUrl(pipelineId);
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
    if (!memcache.delete(userService.getCurrentUser().getUserId() + " " + token)) {
      throw new RuntimeException("Bad token, try again: " + token);
    }
    String bucket = req.getParameter("gcs_bucket");
    if (Strings.isNullOrEmpty(bucket)) {
      bucket = AppIdentityServiceFactory.getAppIdentityService().getDefaultGcsBucketName();
    }

    int entities = parseInt(req.getParameter("entities"));
    int bytesPerEntity = parseInt(req.getParameter("payloadBytesPerEntity"));
    int shardCount = parseInt(req.getParameter("shardCount"));
    PipelineService service = PipelineServiceFactory.newPipelineService();
    redirectToPipelineStatus(resp, service.startNewPipeline(
        new ChainedMapReduceJob(bucket, DATASTORE_TYPE, shardCount, entities, bytesPerEntity)));
  }
}
