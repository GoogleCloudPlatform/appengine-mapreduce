// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl.WORKER_PATH;
import static com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet.makeViewerUrl;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.LocalModulesServiceTestConfig.ModuleInfo;
import com.google.appengine.tools.mapreduce.LocalModulesServiceTestConfig.VersionInfo;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.appengine.tools.pipeline.JobSetting.OnBackend;
import com.google.appengine.tools.pipeline.JobSetting.OnModule;
import com.google.appengine.tools.pipeline.JobSetting.OnQueue;
import com.google.appengine.tools.pipeline.JobSetting.StatusConsoleUrl;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Environment;

import junit.framework.TestCase;

import org.easymock.EasyMock;

import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 */
public class MapReduceJobTest extends TestCase {

  private ModuleInfo module1 =
      new ModuleInfo("module1", new VersionInfo("v1", 10), new VersionInfo("v2", 20));
  private ModuleInfo module2 =
      new ModuleInfo("default", new VersionInfo("1", 1), new VersionInfo("2", 1));
  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig(), new LocalModulesServiceTestConfig(module1, module2),
      new LocalTaskQueueTestConfig());

  @SuppressWarnings("serial")
  private static class DummyMapper extends Mapper<Long, String, Long> {
    @Override
    public void map(Long value) {}
  }

  @Override
  public void setUp() {
    helper.setUp();
    Map<String, Object> attributes = ApiProxy.getCurrentEnvironment().getAttributes();
    @SuppressWarnings("unchecked")
    Map<String, Object> portMap =
        (Map<String, Object>) attributes.get("com.google.appengine.devappserver.portmapping");
    if (portMap == null) {
      portMap = new HashMap<>();
      attributes.put("com.google.appengine.devappserver.portmapping", portMap);
    }
    portMap.put("b1", "backend-hostname");
  }

  public void testBadQueueSettings() throws Exception {
    MapReduceSettings settings = new MapReduceSettings();
    settings.setWorkerQueueName("non-default");
    MapReduceSpecification<Long, String, Long, String, String> specification =
        MapReduceSpecification.of("Empty test MR",
            new NoInput<Long>(1),
            new DummyMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, String>create(),
            new NoOutput<String, String>(1));
    try {
      MapReduceJob.start(specification, settings);
      fail("was expecting failure due to bad queue");
    } catch (RuntimeException ex) {
      assertEquals("The specified queue is unknown : non-default", ex.getMessage());
    }
  }

  @SuppressWarnings("deprecation")
  public void testMakeShardedJobSettings() {
    Key key = KeyFactory.createKey("Kind1", "value1");
    MapReduceSettings mrSettings = new MapReduceSettings();
    mrSettings.setControllerQueueName("bad-quque"); // make sure not used
    mrSettings.setWorkerQueueName("good-queue");
    ShardedJobSettings sjSettings = MapReduceJob.makeShardedJobSettings("job1", mrSettings, key);
    assertNull(sjSettings.getBackend());
    assertEquals("default", sjSettings.getModule());
    assertEquals("1", sjSettings.getVersion());
    assertEquals("localhost", sjSettings.getTaskQueueTarget());
    assertEquals(mrSettings.getWorkerQueueName(), sjSettings.getQueueName());
    assertEquals(getPath(mrSettings, "job1", CONTROLLER_PATH), sjSettings.getControllerPath());
    assertEquals(getPath(mrSettings, "job1", WORKER_PATH), sjSettings.getWorkerPath());
    assertEquals(makeViewerUrl(key, key), sjSettings.getPipelineStatusUrl());
    assertEquals(mrSettings.getMaxShardRetries(), sjSettings.getMaxShardRetries());
    assertEquals(mrSettings.getMaxSliceRetries(), sjSettings.getMaxSliceRetries());

    mrSettings.setBackend("b1");
    sjSettings = MapReduceJob.makeShardedJobSettings("job1", mrSettings, key);
    assertEquals("backend-hostname", sjSettings.getTaskQueueTarget());
    assertEquals("b1", sjSettings.getBackend());
    assertNull(sjSettings.getModule());
    assertNull(sjSettings.getVersion());

    mrSettings.setBackend(null);
    mrSettings.setModule("module1");
    sjSettings = MapReduceJob.makeShardedJobSettings("job1", mrSettings, key);
    assertNull(sjSettings.getBackend());
    assertEquals("module1", sjSettings.getModule());
    assertEquals("v1", sjSettings.getVersion());

    mrSettings.setModule("default");
    Environment env = ApiProxy.getCurrentEnvironment();
    Environment mockEnv = EasyMock.createNiceMock(Environment.class);
    EasyMock.expect(mockEnv.getModuleId()).andReturn("default").atLeastOnce();
    EasyMock.expect(mockEnv.getVersionId()).andReturn("2").atLeastOnce();
    EasyMock.expect(mockEnv.getAttributes()).andReturn(env.getAttributes()).anyTimes();
    EasyMock.replay(mockEnv);
    ApiProxy.setEnvironmentForCurrentThread(mockEnv);
    // Test when current module is the same as requested module
    try {
      sjSettings = MapReduceJob.makeShardedJobSettings("job1", mrSettings, key);
      assertNull(sjSettings.getBackend());
      assertEquals("default", sjSettings.getModule());
      assertEquals("2", sjSettings.getVersion());
    } finally {
      ApiProxy.setEnvironmentForCurrentThread(env);
    }
    EasyMock.verify(mockEnv);
  }

  private String getPath(MapReduceSettings settings, String jobId, String logicPath) {
    return settings.getBaseUrl() + logicPath + "/" + jobId;
  }

  private abstract class Validator<T extends JobSetting, V> {

    private final V expected;

    Validator(V value) {
      expected = value;
    }

    @SuppressWarnings("unchecked")
    void validate(JobSetting value) {
      assertEquals(expected, getValue((T) value));
    }

    @SuppressWarnings("unchecked")
    Class<T> getType() {
      return (Class<T>)
          ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    protected abstract V getValue(T value);
  }

  private class BackendValidator extends Validator<OnBackend, String> {

    BackendValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(OnBackend value) {
      return value.getValue();
    }
  }

  private class ModuleValidator extends Validator<OnModule, String> {

    ModuleValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(OnModule value) {
      return value.getValue();
    }
  }

  private class QueueValidator extends Validator<OnQueue, String> {

    QueueValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(OnQueue value) {
      return value.getValue();
    }
  }

  private class StatusConsoleValidator extends Validator<StatusConsoleUrl, String> {

    StatusConsoleValidator(String value) {
      super(value);
    }

    @Override
    protected String getValue(StatusConsoleUrl value) {
      return value.getValue();
    }
  }

  public void testPipelineSettings() {
    MapReduceSettings mrSettings = new MapReduceSettings().setWorkerQueueName("queue1");
    verifyPipelineSettings(MapReduceJob.makeJobSettings(mrSettings),
        new BackendValidator(null), new ModuleValidator(null), new QueueValidator("queue1"));
    mrSettings = new MapReduceSettings().setBackend("backend1");
    verifyPipelineSettings(MapReduceJob.makeJobSettings(mrSettings),
        new BackendValidator("backend1"), new ModuleValidator(null), new QueueValidator("default"));
    mrSettings = new MapReduceSettings().setModule("m1");
    verifyPipelineSettings(MapReduceJob.makeJobSettings(mrSettings, new StatusConsoleUrl("u1")),
        new BackendValidator(null), new ModuleValidator("m1"),
        new QueueValidator("default"), new StatusConsoleValidator("u1"));
  }

  @SafeVarargs
  final void verifyPipelineSettings(
      JobSetting[] settings, Validator<? extends JobSetting, ?>... validators) {
    Map<Class<? extends JobSetting>, Validator<? extends JobSetting, ?>> expected = new HashMap<>();
    for (Validator<? extends JobSetting, ?> v : validators) {
      expected.put(v.getType(), v);
    }
    Set<Class<? extends JobSetting>> unique = new HashSet<>();
    for (JobSetting setting : settings) {
      Class<? extends JobSetting> settingClass = setting.getClass();
      unique.add(settingClass);
      expected.get(settingClass).validate(setting);
    }
    assertEquals(expected.size(), unique.size());
  }
}
