// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.v2.impl.handlers;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.taskqueue.dev.LocalTaskQueue;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo;
import com.google.appengine.api.taskqueue.dev.QueueStateInfo.TaskStateInfo;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.AppEngineJobContext;
import com.google.appengine.tools.mapreduce.AppEngineMapper;
import com.google.appengine.tools.mapreduce.AppEngineTaskAttemptContext;
import com.google.appengine.tools.mapreduce.ConfigurationXmlUtil;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.QuotaConsumer;
import com.google.appengine.tools.mapreduce.QuotaManager;
import com.google.appengine.tools.mapreduce.StubInputFormat;
import com.google.appengine.tools.mapreduce.StubInputSplit;
import com.google.appengine.tools.mapreduce.StubMapper;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;
import com.google.appengine.tools.mapreduce.v2.impl.ShardState;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;

import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 *
 */
public class WorkerTest extends TestCase {
  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
          new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

    private DatastoreService ds;

    @Override
    public void setUp() throws Exception {
      super.setUp();
      helper.setUp();
      ds = DatastoreServiceFactory.getDatastoreService();
      StubMapper.fakeSetUp();
    }

    @Override
    public void tearDown() throws Exception {
      StubMapper.fakeTearDown();
      helper.tearDown();
      super.tearDown();
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

    private static HttpServletRequest createMockStartRequest(Configuration conf) {
      HttpServletRequest request = createMockRequest(MapReduceServlet.START_PATH, false, false);
      expect(request.getParameter(AppEngineJobContext.CONFIGURATION_PARAMETER_NAME))
        .andReturn(ConfigurationXmlUtil.convertConfigurationToXml(conf))
        .anyTimes();
      return request;
    }

    private static HttpServletRequest createMockMapperWorkerRequest(int sliceNumber,
        JobID jobId, TaskAttemptID taskAttemptId) {
      HttpServletRequest request = createMockRequest(MapReduceServlet.CONTROLLER_PATH, true, false);
      expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
        .andReturn("" + sliceNumber)
        .anyTimes();
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
      expect(request.getParameter(AppEngineTaskAttemptContext.TASK_ATTEMPT_ID_PARAMETER_NAME))
        .andReturn(taskAttemptId.toString())
        .anyTimes();
      return request;
    }

    private static Configuration getSampleMapReduceConfiguration() {
      Configuration conf = new Configuration(false);
      // TODO(user): If I can find a way to keep the test small
      // I'd like to exercise the non-default queues, but currently
      // it looks like the test harness only supports an actual queues.xml.
      conf.set(AppEngineJobContext.CONTROLLER_QUEUE_KEY, "default");
      conf.set(AppEngineJobContext.WORKER_QUEUE_KEY, "default");
      conf.set(AppEngineJobContext.MAPPER_SHARD_COUNT_KEY, "2");
      conf.set(AppEngineJobContext.MAPPER_INPUT_PROCESSING_RATE_KEY, "1000");
      conf.setClass("mapreduce.inputformat.class", StubInputFormat.class, InputFormat.class);
      conf.setClass("mapreduce.map.class", StubMapper.class, AppEngineMapper.class);
      return conf;
    }

    // Creates an MR state with an empty configuration and the given job ID,
    // and stores it in the datastore.
    private void persistMRState(JobID jobId, Configuration conf) {
      MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(ds, "", jobId, 0);
      mrState.setConfigurationXML(ConfigurationXmlUtil.convertConfigurationToXml(conf));
      mrState.persist();
    }

    public void testScheduleShards() {
      JobID jobId = new JobID("123", 1);
      HttpServletRequest request = createMockStartRequest(getSampleMapReduceConfiguration());
      expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
      replay(request);

      Configuration conf = getSampleMapReduceConfiguration();
      persistMRState(jobId, conf);
      AppEngineJobContext context = new AppEngineJobContext(request);
      List<InputSplit> splits = Arrays.asList(
          (InputSplit) new StubInputSplit(1), new StubInputSplit(2));
      StubInputFormat format = new StubInputFormat();
      Worker.scheduleShards(context, format, splits, MapReduceServlet.getBase(request));
      QueueStateInfo defaultQueue = getDefaultQueueInfo();
      assertEquals(2, defaultQueue.getCountTasks());
      TaskStateInfo firstTask = defaultQueue.getTaskInfo().get(0);
      assertEquals("/mapreduce/mapperCallback", firstTask.getUrl());
      assertTrue(firstTask.getBody(),
          firstTask.getBody().indexOf("taskAttemptID=attempt_123_0001_m_000000") != -1);
      verify(request);
    }

    private static QueueStateInfo getDefaultQueueInfo() {
      LocalTaskQueue localQueue
          = (LocalTaskQueue) LocalServiceTestHelper.getLocalService(
              LocalTaskQueue.PACKAGE);
      QueueStateInfo defaultQueue = localQueue.getQueueStateInfo().get("default");
      return defaultQueue;
    }

    /**
     * Test that a mapper worker behaves gracefully without quota.
     */
    public void testMapperWorker_withoutQuota() {
      JobID jobId = new JobID("foo", 1);
      TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(jobId, true, 0), 0);

      HttpServletRequest request = createMockMapperWorkerRequest(0, jobId, taskAttemptId);

      replay(request);

      Configuration conf = getSampleMapReduceConfiguration();
      persistMRState(jobId, conf);

      ShardState state = ShardState.generateInitializedShardState(ds, taskAttemptId);
      StubInputFormat format = new StubInputFormat();
      AppEngineJobContext context = new AppEngineJobContext(conf, jobId, request);

      AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
          context, state, taskAttemptId);
      List<InputSplit> splits = format.getSplits(context);
      state.setInputSplit(conf, splits.get(0));
      RecordReader<IntWritable, IntWritable> reader = format.createRecordReader(
          splits.get(0), taskAttemptContext);
      state.setRecordReader(conf, reader);
      state.persist();
      Worker.handleMapperWorker(request);

      // No quota so shouldn't invoke any maps.
      assertEquals(0, StubMapper.invocationKeys.size());

      QueueStateInfo defaultQueue = getDefaultQueueInfo();
      assertEquals(1, defaultQueue.getCountTasks());
      TaskStateInfo firstTask = defaultQueue.getTaskInfo().get(0);
      assertEquals("/mapreduce/mapperCallback", firstTask.getUrl());
      assertTrue(firstTask.getBody(),
          firstTask.getBody().indexOf("sliceNumber=1") != -1);

      assertTrue(StubMapper.setupCalled);
      assertFalse(StubMapper.cleanupCalled);

      // Per task handler shouldn't be called if there's no quota to begin with.
      assertFalse(StubMapper.taskSetupCalled);
      assertFalse(StubMapper.taskCleanupCalled);

      verify(request);
    }

    public void testMapperWorker_withQuota() {
      JobID jobId = new JobID("foo", 1);
      TaskAttemptID taskAttemptId = new TaskAttemptID(new TaskID(jobId, true, 0), 0);

      HttpServletRequest request = createMockMapperWorkerRequest(0, jobId, taskAttemptId);

      replay(request);

      Configuration conf = getSampleMapReduceConfiguration();
      persistMRState(jobId, conf);

      ShardState state = ShardState.generateInitializedShardState(ds, taskAttemptId);
      StubInputFormat format = new StubInputFormat();
      AppEngineJobContext context = new AppEngineJobContext(conf, jobId, request);

      AppEngineTaskAttemptContext taskAttemptContext = new AppEngineTaskAttemptContext(
          context, state, taskAttemptId);
      List<InputSplit> splits = format.getSplits(context);
      state.setInputSplit(conf, splits.get(0));
      RecordReader<IntWritable, IntWritable> reader = format.createRecordReader(
          splits.get(0), taskAttemptContext);
      state.setRecordReader(conf, reader);
      state.persist();
      new QuotaManager(MemcacheServiceFactory.getMemcacheService()).put(
          taskAttemptId.toString(), 10);
      Worker.handleMapperWorker(request);
      assertEquals(((StubInputSplit) splits.get(0)).getKeys(),
          StubMapper.invocationKeys);
      verify(request);
    }

    // Ensures that being out of quota on the consume call doesn't
    // skip entities.
    @SuppressWarnings("unchecked")
    public void testProcessMapper_noQuotaDoesntSkip() throws Exception {
      AppEngineMapper mapper = createMock(AppEngineMapper.class);
      QuotaConsumer consumer = createMock(QuotaConsumer.class);

      // Process mapper should both start and cleanup the task,
      // but shouldn't actually get entities because it's out of quota.
      mapper.taskSetup(null);
      mapper.taskCleanup(null);

      // Simulate having quota to start and then running out
      expect(consumer.check(1)).andReturn(true);
      expect(consumer.consume(1)).andReturn(false);
      replay(mapper, consumer);

      Worker.processMapper(mapper, null, consumer, System.currentTimeMillis() + 10000);

      verify(mapper, consumer);
    }
}
