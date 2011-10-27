/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import javax.servlet.http.HttpServletRequest;

/**
 * Tests {@link AppEngineJobContext}.
 *
 */
public class AppEngineJobContextTest extends TestCase {
  private static final String SIMPLE_CONF_XML =
    "<?xml version=\"1.0\"?>"
    + "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>"
    + "<configuration>"
    + "<property>"
    + "<name>foo.bar</name>"
    + "<value>/tmp/foo</value>"
    + "</property>"
    + "</configuration>";

  private final LocalServiceTestHelper helper =
    new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig(),
        new LocalTaskQueueTestConfig(), new LocalMemcacheServiceTestConfig());

  private DatastoreService ds;

  private JobID jobId;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    ds = DatastoreServiceFactory.getDatastoreService();
    jobId = new JobID("foo", 1);
  }

  @Override
  public void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testGetConfigurationFromRequest() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    expect(req.getParameter(AppEngineJobContext.CONFIGURATION_PARAMETER_NAME))
        .andReturn(SIMPLE_CONF_XML);
    replay(req);

    Configuration conf = AppEngineJobContext.getConfigurationFromRequest(req, true);
    assertEquals("/tmp/foo", conf.get("foo.bar"));
    verify(req);
  }

  public void testGetJobContextFromRequest() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    JobID jobId = new JobID("foo", 1);
    expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
    replay(req);


    Configuration conf = ConfigurationXmlUtil.getConfigurationFromXml(SIMPLE_CONF_XML);
    persistMRState(jobId, conf);

    JobContext context = new AppEngineJobContext(req, false);
    assertEquals("/tmp/foo", context.getConfiguration().get("foo.bar"));
    assertEquals(jobId.toString(), context.getJobID().toString());
    verify(req);
  }

  // Creates an MR state with an empty configuration and the given job ID,
  // and stores it in the datastore.
  private void persistMRState(JobID jobId, Configuration conf) {
    MapReduceState mrState = MapReduceState.generateInitializedMapReduceState(ds, "", jobId, 0);
    mrState.setConfigurationXML(ConfigurationXmlUtil.convertConfigurationToXml(conf));
    mrState.persist();
  }

  /**
   * Ensures that {@link AppEngineJobContext#getWorkerQueue()} falls back
   * to the default queue.
   */
  public void testGetQueueDefault() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    expect(req.getHeader("X-AppEngine-QueueName")).andReturn(null);
    expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
    replay(req);

    Configuration conf = new Configuration(false);
    persistMRState(jobId, conf);

    AppEngineJobContext context = new AppEngineJobContext(req, false);
    assertEquals("default", context.getWorkerQueue().getQueueName());
    verify(req);
  }

  /**
   * Ensures that {@link AppEngineJobContext#getWorkerQueue()} uses the current
   * task queue if set to non-default.
   */
  public void testGetQueueRequest() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    expect(req.getHeader("X-AppEngine-QueueName")).andReturn("bar");
    expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
      .andReturn(jobId.toString())
      .anyTimes();
    replay(req);

    Configuration conf = new Configuration(false);
    persistMRState(jobId, conf);

    AppEngineJobContext context = new AppEngineJobContext(req, false);
    assertEquals("bar", context.getWorkerQueue().getQueueName());
    verify(req);
  }

  /**
   * Ensures that {@link AppEngineJobContext#getWorkerQueue()} uses the worker
   * queue specified in the job's configuration.
   */
  public void testGetQueueConfiguration() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    expect(req.getHeader("X-AppEngine-QueueName")).andReturn("bar");
    expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
      .andReturn(jobId.toString())
      .anyTimes();
    replay(req);

    Configuration conf = new Configuration(false);
    // Configuration value should take precedence
    conf.set(AppEngineJobContext.WORKER_QUEUE_KEY, "baz");
    persistMRState(jobId, conf);

    AppEngineJobContext context = new AppEngineJobContext(req, false);
    assertEquals("baz", context.getWorkerQueue().getQueueName());
    verify(req);
  }

  /**
   * Ensures that {@link AppEngineJobContext#getControllerQueue()} uses the
   * controller queue specified in the job's configuration.
   */
  public void testGetQueueConfigurationController() {
    HttpServletRequest req = createMock(HttpServletRequest.class);
    expect(req.getHeader("X-AppEngine-QueueName")).andReturn("bar");
    expect(req.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
    replay(req);

    Configuration conf = new Configuration(false);
    // Configuration value should take precedence
    conf.set(AppEngineJobContext.CONTROLLER_QUEUE_KEY, "baz");
    persistMRState(jobId, conf);

    AppEngineJobContext context = new AppEngineJobContext(req, false);
    assertEquals("baz", context.getControllerQueue().getQueueName());
    verify(req);
  }
}
