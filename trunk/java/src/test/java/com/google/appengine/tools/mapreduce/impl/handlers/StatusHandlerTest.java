// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.EndToEndTestCase;
import com.google.appengine.tools.mapreduce.impl.AbstractWorkerController;
import com.google.appengine.tools.mapreduce.impl.TestWorkerTask;
import com.google.appengine.tools.mapreduce.impl.WorkerResult;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobService;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobServiceFactory;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobState;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.json.JSONObject;

import java.io.Serializable;

/**
 *
 */
public class StatusHandlerTest extends EndToEndTestCase {

  private static final class DummyWorkerController
      extends AbstractWorkerController<TestWorkerTask, Integer> {
    private static final long serialVersionUID = 1L;

    private DummyWorkerController(String shardedJobName) {
      super(shardedJobName);
    }

    @Override
    public void completed(WorkerResult<Integer> finalCombinedResult) {}

    @Override
    public void failed(Status status) {}
  }

  // Tests that an job that has just been initialized returns a reasonable job detail.
  public void testGetJobDetail_empty() throws Exception {
    ShardedJobService jobService = ShardedJobServiceFactory.getShardedJobService();
    ShardedJobSettings settings = new ShardedJobSettings();
    ShardedJobController<TestWorkerTask, WorkerResult<Integer>>
    controller = new DummyWorkerController("Namey");
    jobService.startJob(
        "testGetJobDetail_empty", ImmutableList.<TestWorkerTask>of(), controller, settings);

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
    ShardedJobSettings settings = new ShardedJobSettings();
    ShardedJobController<TestWorkerTask, WorkerResult<Integer>> controller =
        new DummyWorkerController("Namey");
    TestWorkerTask three = new TestWorkerTask(0, 3, null);
    TestWorkerTask two = new TestWorkerTask(1, 2, null);
    TestWorkerTask one = new TestWorkerTask(0, 1, three);
    jobService.startJob(
        "testGetJobDetail_populated", ImmutableList.of(one, two), controller, settings);
    ShardedJobState<?, WorkerResult<? extends Serializable>> state =
        jobService.getJobState("testGetJobDetail_populated");
    assertEquals(2, state.getActiveTaskCount());
    assertTrue(Iterables.isEmpty(state.getAggregateResult().getCounters().getCounters()));
    assertEquals(2, state.getTotalTaskCount());
    assertEquals(new Status(Status.StatusCode.RUNNING), state.getStatus());
    JSONObject jobDetail = StatusHandler.handleGetJobDetail("testGetJobDetail_populated");
    assertNotNull(jobDetail);
    assertEquals("testGetJobDetail_populated", jobDetail.getString("mapreduce_id"));
    assertEquals("Namey", jobDetail.getString("name"));
    assertEquals(true, jobDetail.getBoolean("active"));
    assertEquals(2, jobDetail.getInt("active_shards"));
    assertTrue(
        jobDetail.toString().matches(
            "\\{\"mapreduce_id\":\"testGetJobDetail_populated\"," +
                "\"shards\":\\[\\{\"shard_description\":\"\"," +
                "\"active\":false," +
                "\"result_status\":\"initializing\"," +
                "\"shard_number\":0\\}," +
                "\\{\"shard_description\":\"\"," +
                "\"active\":false," +
                "\"result_status\":\"initializing\"," +
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
    assertTrue(
        jobDetail.toString().matches(
            "\\{\"mapreduce_id\":\"testGetJobDetail_populated\"," +
                "\"shards\":\\[\\{" +
                "\"last_work_item\":\"3\"," +
                "\"shard_description\":\"\"," +
                "\"active\":true," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"shard_number\":0\\}," +
                "\\{\"last_work_item\":\"2\"," +
                "\"shard_description\":\"\"," +
                "\"active\":true," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"shard_number\":1\\}\\]," +
                "\"mapper_spec\":\\{\"mapper_params\":\\{\"Shards total\":2," +
                "\"Shards active\":0," +
                "\"Shards completed\":2\\}\\}," +
                "\"name\":\"Namey\"," +
                "\"active\":false," +
                "\"active_shards\":0," +
                "\"updated_timestamp_ms\":[0-9]*," +
                "\"chart_url\":\"[^\"]*\"," +
                "\"counters\":\\{\"TestWorkerTaskSum\":6\\}," +
                "\"start_timestamp_ms\":[0-9]*\\," +
                "\"result_status\":\"DONE\"}"));
  }

  // -------------------------- STATIC METHODS --------------------------

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
