// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.handlers;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.MapReduceServlet;

import junit.framework.TestCase;

import org.json.JSONObject;

import javax.servlet.http.HttpServletRequest;

/**
 *
 */
public class StatusHandlerTest extends TestCase {
// ------------------------------ FIELDS ------------------------------

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

    private MapReduceServlet servlet;

    private DatastoreService ds;

// ------------------------ OVERRIDING METHODS ------------------------

    @Override
    public void setUp() throws Exception {
      super.setUp();
      helper.setUp();
      ds = DatastoreServiceFactory.getDatastoreService();
      servlet = new MapReduceServlet();
    }

    @Override
    public void tearDown() throws Exception {
      helper.tearDown();
      super.tearDown();
    }

// -------------------------- TEST METHODS --------------------------

    public void testCleanupJob() throws Exception {
/*
      MapperStateEntity state = MapperStateEntity.createForNewJob(
          ds, "Namey", new JobID("14", 28).toString(), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      JSONObject response = StatusHandler.handleCleanupJob(state.getJobId());
      try {
        MapperStateEntity.getMapReduceStateFromJobID(ds, JobID.forName(state.getJobId()));
        fail("MapperStateEntity entity should have been removed from the datastore");
      } catch (EntityNotFoundException ignored) {
        // expected
      }
      assertTrue("Response has \"uccess\" in the status: " + response,
          ((String) response.get("status")).contains("uccess"));
*/
    }

    // Tests that an job that has just been initialized returns a reasonable
    // job detail.
    public void testGetJobDetail_empty() throws Exception {
/*
      MapperStateEntity state = MapperStateEntity.createForNewJob(
          ds, "Namey", new JobID("14", 28).toString(), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      JSONObject result = StatusHandler.handleGetJobDetail(state.getJobId());
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
*/
    }

    // Tests that a populated job (with a couple of shards) generates a reasonable
    // job detail.
    public void testGetJobDetail_populated() throws Exception {
/*
      MapperStateEntity state = MapperStateEntity.createForNewJob(
          ds, "Namey", new JobID("14", 28).toString(), 12);
      state.setConfigurationXML(
          ConfigurationXmlUtil.convertConfigurationToXml(new Configuration(false)));
      state.persist();
      state.setLastPollTime(24);

      // shard1 represents a shard that has lived a full and complete life,
      // with its trusty counter, binky, and a status message left by its
      // loving children.
      ShardStateEntity shard1 = ShardStateEntity.createForNewJob(ds,
          new TaskAttemptID(new TaskID(JobID.forName(state.getJobId()), true, 1), 1));
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
      ShardStateEntity shard2 = ShardStateEntity.createForNewJob(ds,
          new TaskAttemptID(new TaskID(JobID.forName(state.getJobId()), true, 2), 1));
      shard2.setClock(new MockClock(77));
      shard2.setInputSplit(new Configuration(false), new StubInputSplit());
      shard2.setRecordReader(new Configuration(false), new StubRecordReader());

      shard2.persist();

      JSONObject result = StatusHandler.handleGetJobDetail(state.getJobId());
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
*/
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
}
