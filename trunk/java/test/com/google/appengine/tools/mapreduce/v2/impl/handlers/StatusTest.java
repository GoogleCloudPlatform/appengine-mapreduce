// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.MockClock;
import com.google.appengine.tools.mapreduce.StubInputSplit;
import com.google.appengine.tools.mapreduce.StubMapper;
import com.google.appengine.tools.mapreduce.StubRecordReader;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.easymock.EasyMock;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 */
public class StatusTest extends TestCase {
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

    private MapReduceServlet servlet;

    private DatastoreService ds;

    @Override
    public void setUp() throws Exception {
      super.setUp();
      helper.setUp();
      ds = DatastoreServiceFactory.getDatastoreService();
      servlet = new MapReduceServlet();
      StubMapper.fakeSetUp();
    }

    @Override
    public void tearDown() throws Exception {
      StubMapper.fakeTearDown();
      helper.tearDown();
      super.tearDown();
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

    private static HttpServletRequest createMockRequest(
        String handler, boolean taskQueueRequest, boolean ajaxRequest) {
      HttpServletRequest request = createMock(HttpServletRequest.class);
      if (taskQueueRequest) {
        expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn("default")
          .anyTimes();
      } else {
        expect(request.getHeader("X-AppEngine-QueueName"))
          .andReturn(null)
          .anyTimes();
      }
      if (ajaxRequest) {
        expect(request.getHeader("X-Requested-With"))
          .andReturn("XMLHttpRequest")
          .anyTimes();
      } else {
        expect(request.getHeader("X-Requested-With"))
          .andReturn(null)
          .anyTimes();
      }
      expect(request.getRequestURI())
        .andReturn("/mapreduce/" + handler)
        .anyTimes();
      return request;
    }

    public void testControllerCSRF() {
      JobID jobId = new JobID("foo", 1);
      // Send it as an AJAX request but not a task queue request - should be denied.
      HttpServletRequest request = createMockRequest(MapReduceServlet.CONTROLLER_PATH, false, true);
      expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
        .andReturn("" + 0)
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn("" + jobId)
        .anyTimes();
      HttpServletResponse response = createMock(HttpServletResponse.class);
      try {
        response.sendError(403, "Received unexpected non-task queue request.");
      } catch (IOException ioe) {
        // Can't actually be sent in mock setup
      }
      replay(request, response);
      servlet.doPost(request, response);
      verify(request, response);
    }

    public void testGetJobDetailCSRF() {
      JobID jobId = new JobID("foo", 1);
      // Send it as a task queue request but not an ajax request - should be denied.
      HttpServletRequest request = createMockRequest(
          MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.GET_JOB_DETAIL_PATH, true, false);
      expect(request.getMethod())
        .andReturn("POST")
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn("" + jobId)
        .anyTimes();

      HttpServletResponse response = createMock(HttpServletResponse.class);

      // Set before error and last one wins, so this is harmless.
      response.setContentType("application/json");
      EasyMock.expectLastCall().anyTimes();

      try {
        response.sendError(403, "Received unexpected non-XMLHttpRequest command.");
      } catch (IOException ioe) {
        // Can't actually be sent in mock setup
      }
      replay(request, response);
      servlet.doGet(request, response);
      verify(request, response);
    }

    // Tests that an job that has just been initialized returns a reasonable
    // job detail.
    public void testGetJobDetail_empty() throws Exception {
      MapReduceState state = MapReduceState.generateInitializedMapReduceState(
          ds, "Namey", new JobID("14", 28), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      JSONObject result = Status.handleGetJobDetail(state.getJobID());
      assertJsonEquals(
            "{'mapreduce_id':'job_14_0028',"
          + " 'shards':[],"
          + " 'mapper_spec':{'mapper_params':{}},"
          + " 'name':'Namey',"
          + " 'active':true,"
          + " 'updated_timestamp_ms':-1,"
          + " 'chart_url':'',"
          + " 'configuration':'<?xml version=\\'1.0\\' encoding=\\'UTF-8\\' standalone=\\'no\\'?>"
          +                   "<configuration><\\/configuration>',"
          + " 'counters':{},"
          + " 'start_timestamp_ms':12,"
          + " 'result_status': 'ACTIVE'}", result);
    }

    // Tests that a populated job (with a couple of shards) generates a reasonable
    // job detail.
    public void testGetJobDetail_populated() throws Exception {
      MapReduceState state = MapReduceState.generateInitializedMapReduceState(
          ds, "Namey", new JobID("14", 28), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      state.setLastPollTime(24);

      // shard1 represents a shard that has lived a full and complete life,
      // with its trusty counter, binky, and a status message left by its
      // loving children.
      ShardState shard1 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(JobID.forName(state.getJobID()), true, 1), 1));
      shard1.setClock(new MockClock(45));
      shard1.setInputSplit(new Configuration(false), new StubInputSplit());
      shard1.setRecordReader(new Configuration(false), new StubRecordReader());
      shard1.setStatusString("Daddy!");

      Counters counters1 = new Counters();
      counters1.findCounter("binky", "winky").increment(1);
      shard1.setCounters(counters1);

      shard1.setDone();
      shard1.persist();

      // shard2 represents the minimal shard that should still work
      ShardState shard2 = ShardState.generateInitializedShardState(ds,
          new TaskAttemptID(new TaskID(JobID.forName(state.getJobID()), true, 2), 1));
      shard2.setClock(new MockClock(77));
      shard2.setInputSplit(new Configuration(false), new StubInputSplit());
      shard2.setRecordReader(new Configuration(false), new StubRecordReader());

      shard2.persist();

      JSONObject result = Status.handleGetJobDetail(state.getJobID());
      assertJsonEquals(
            "{'mapreduce_id':'job_14_0028',"
          + " 'shards':[{'shard_description':'Daddy!',"
          + "            'active':false,"
          + "            'updated_timestamp_ms':45,"
          + "            'result_status': 'DONE',"
          + "            'shard_number':1},"
          + "           {'shard_description':'',"
          + "            'active':true,"
          + "            'updated_timestamp_ms':77,"
          + "            'result_status': 'ACTIVE',"
          + "            'shard_number':2}],"
          + " 'mapper_spec':{'mapper_params':{}},"
          + " 'name':'Namey',"
          + " 'active':true,"
          + " 'updated_timestamp_ms':-1,"
          + " 'chart_url':'',"
          + " 'configuration':'<?xml version=\\'1.0\\' encoding=\\'UTF-8\\' standalone=\\'no\\'?>"
          +                   "<configuration><\\/configuration>',"
          + " 'counters':{},"
          + " 'start_timestamp_ms':12,"
          + " 'result_status': 'ACTIVE'}", result);
    }

    public void testCleanupJob() throws Exception {
      MapReduceState state = MapReduceState.generateInitializedMapReduceState(
          ds, "Namey", new JobID("14", 28), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      JSONObject response = Status.handleCleanupJob(state.getJobID());
      try {
        MapReduceState.getMapReduceStateFromJobID(ds, JobID.forName(state.getJobID()));
        fail("MapReduceState entity should have been removed from the datastore");
      } catch (EntityNotFoundException ignored) {
        // expected
      }
      assertTrue("Response has \"uccess\" in the status: " + response,
          ((String) response.get("status")).contains("uccess"));
    }

    public void testCommandError() throws Exception {
      HttpServletRequest request = createMockRequest(
          MapReduceServlet.COMMAND_PATH + "/" + MapReduceServlet.GET_JOB_DETAIL_PATH, false, true);
      expect(request.getMethod())
        .andReturn("GET")
        .anyTimes();
      HttpServletResponse response = createMock(HttpServletResponse.class);
      PrintWriter responseWriter = createMock(PrintWriter.class);
      responseWriter.write('{');
      responseWriter.write("\"error_class\"");
      responseWriter.write(':');
      responseWriter.write("\"java.lang.RuntimeException\"");
      responseWriter.write(',');
      responseWriter.write("\"error_message\"");
      responseWriter.write(':');
      responseWriter.write("\"Full stack trace is available in the server logs. "
          + "Message: blargh\"");
      responseWriter.write('}');
      responseWriter.flush();
      // This method can't actually throw this exception, but that's not
      // important to the test.
      expect(request.getParameter("mapreduce_id")).andThrow(new RuntimeException("blargh"));
      response.setContentType("application/json");
      expect(response.getWriter()).andReturn(responseWriter).anyTimes();
      replay(request, response, responseWriter);
      servlet.doPost(request, response);
      verify(request, response, responseWriter);
    }
}
