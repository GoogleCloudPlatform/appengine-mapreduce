// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.FetchOptions;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobService;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.shardedjob.TestTask;
import com.google.common.collect.ImmutableList;

import org.json.JSONObject;

import java.util.List;
import java.util.Random;

/**
 *
 */
public class StatusHandlerTest extends EndToEndTestCase {

  private static final class DummyWorkerController
      extends ShardedJobController<TestTask> {
    private static final long serialVersionUID = 1L;

    private DummyWorkerController(String shardedJobName) {
      super(shardedJobName);
    }

    @Override
    public void failed(Status status) {}

    @Override
    public void completed(List<? extends TestTask> results) {}
  }

  public void testCleanupJob() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    assertTrue(jobService.cleanupJob("testCleanupJob")); // No such job yet
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController("testCleanupJob");
    byte[] bytes = new byte[1024 * 1024];
    new Random().nextBytes(bytes);
    TestTask s1 = new TestTask(0, 2, 2, 2, bytes);
    TestTask s2 = new TestTask(1, 2, 2, 1);
    jobService.startJob("testCleanupJob", ImmutableList.of(s1, s2), controller, settings);
    assertFalse(jobService.cleanupJob("testCleanupJob"));
    executeTasksUntilEmpty();
    DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
    assertEquals(9, ds.prepare(new Query()).countEntities(FetchOptions.Builder.withDefaults()));
    assertTrue(jobService.cleanupJob("testCleanupJob"));
    executeTasksUntilEmpty();
    assertEquals(0, ds.prepare(new Query()).countEntities(FetchOptions.Builder.withDefaults()));
  }

  // Tests that an job that has just been initialized returns a reasonable job detail.
  public void testGetJobDetail_empty() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController("Namey");
    jobService.startJob("testGetJobDetail_empty", ImmutableList.<TestTask>of(), controller,
        settings);

    JSONObject result = StatusHandler.handleGetJobDetail("testGetJobDetail_empty");
    assertEquals("testGetJobDetail_empty", result.getString("mapreduce_id"));
    assertEquals(0, result.getJSONArray("shards").length());
    assertNotNull(result.getJSONObject("mapper_spec"));
    assertEquals("Namey", result.getString("name"));
    assertEquals(0, result.getJSONObject("counters").length());
  }

  // Tests that a populated job (with a couple of shards) generates a reasonable job detail.
  public void testGetJobDetail_populated() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    ShardedJobSettings settings = new ShardedJobSettings.Builder().build();
    ShardedJobController<TestTask> controller = new DummyWorkerController("Namey");
    TestTask s1 = new TestTask(0, 2, 2, 2);
    TestTask s2 = new TestTask(1, 2, 2, 1);
    jobService.startJob(
        "testGetJobDetail_populated", ImmutableList.of(s1, s2), controller, settings);
    ShardedJobState state = jobService.getJobState("testGetJobDetail_populated");
    assertEquals(2, state.getActiveTaskCount());
    assertEquals(2, state.getTotalTaskCount());
    assertEquals(new Status(Status.StatusCode.RUNNING), state.getStatus());
    JSONObject jobDetail = StatusHandler.handleGetJobDetail("testGetJobDetail_populated");
    assertNotNull(jobDetail);
    assertEquals("testGetJobDetail_populated", jobDetail.getString("mapreduce_id"));
    assertEquals("Namey", jobDetail.getString("name"));
    assertEquals(true, jobDetail.getBoolean("active"));
    assertEquals(2, jobDetail.getInt("active_shards"));
    assertTrue(jobDetail.toString(),
        jobDetail.toString().matches(
            "\\{\"mapreduce_id\":\"testGetJobDetail_populated\"," +
                "\"chart_width\":300," +
                "\"shards\":\\[\\{\"shard_description\":\"[^\"]*\"," +
                "\"active\":true," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"shard_number\":0\\}," +
                "\\{\"shard_description\":\"[^\"]*\"," +
                "\"active\":true," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"shard_number\":1\\}\\]," +
                "\"mapper_spec\":\\{\"mapper_params\":\\{\"Shards total\":2," +
                "\"Shards active\":2," +
                "\"Shards completed\":0\\}\\}," +
                "\"name\":\"Namey\"," +
                "\"active\":true," +
                "\"active_shards\":2," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"chart_url\":\"[^\"]*\"," +
                "\"counters\":\\{\\}," +
            "\"start_timestamp_ms\":[0-9]*\\}"));


    executeTasksUntilEmpty();

    jobDetail = StatusHandler.handleGetJobDetail("testGetJobDetail_populated");
    assertNotNull(jobDetail);
    assertEquals("testGetJobDetail_populated", jobDetail.getString("mapreduce_id"));
    assertEquals("Namey", jobDetail.getString("name"));
    assertEquals(false, jobDetail.getBoolean("active"));
    assertEquals(0, jobDetail.getInt("active_shards"));
    assertTrue(jobDetail.toString(),
        jobDetail.toString().matches(
            "\\{\"mapreduce_id\":\"testGetJobDetail_populated\"," +
                "\"chart_width\":300," +
                "\"shards\":\\[\\{" +
                "\"shard_description\":\"[^\"]*\"," +
                "\"active\":false," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"result_status\":\"DONE\"," +
                "\"shard_number\":0\\}," +
                "\\{\"shard_description\":\"[^\"]*\"," +
                "\"active\":false," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"result_status\":\"DONE\"," +
                "\"shard_number\":1\\}\\]," +
                "\"mapper_spec\":\\{\"mapper_params\":\\{\"Shards total\":2," +
                "\"Shards active\":0," +
                "\"Shards completed\":2\\}\\}," +
                "\"name\":\"Namey\"," +
                "\"active\":false," +
                "\"active_shards\":0," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"chart_url\":\"[^\"]*\"," +
                "\"counters\":\\{\"TestTaskSum\":6\\}," +
                "\"start_timestamp_ms\":[0-9]*\\," +
                "\"result_status\":\"DONE\"}"));
  }

  /**
   * Compares a string representation of the expected JSON object
   * with the actual, ignoring white space and converting single quotes
   * to double quotes.
   */
  public static void assertJsonEquals(String expected, JSONObject actual) {
    assertEquals(expected.replace('\'', '"').replace(" ", ""),
        actual.toString().replace(" ", "").replace("\\r\\n", "").replace("\\n", ""));
  }
}
