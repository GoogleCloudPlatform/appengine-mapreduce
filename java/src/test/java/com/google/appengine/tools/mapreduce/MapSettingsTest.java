// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.MapSettings.CONTROLLER_PATH;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_MILLIS_PER_SLICE;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_TIMEOUT_RATIO;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETRIES;
import static com.google.appengine.tools.mapreduce.MapSettings.WORKER_PATH;
import static com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet.makeViewerUrl;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobSettings;
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
@SuppressWarnings("deprecation")
public class MapSettingsTest extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig(),
      new LocalTaskQueueTestConfig(),
      new LocalModulesServiceTestConfig()
        .addBasicScalingModuleVersion("module1", "v1", 10)
        .addBasicScalingModuleVersion("module1", "v2", 10)
        .addBasicScalingModuleVersion("default", "1", 1)
        .addBasicScalingModuleVersion("default", "2", 1));

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

  public void testDefaultSettings() {
    MapSettings mrSettings = new MapSettings.Builder().build();
    assertNull(mrSettings.getBackend());
    assertNull(mrSettings.getModule());
    assertNull(mrSettings.getWorkerQueueName());
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals(DEFAULT_MILLIS_PER_SLICE, mrSettings.getMillisPerSlice());
    assertEquals(DEFAULT_SHARD_RETRIES, mrSettings.getMaxShardRetries());
    assertEquals(DEFAULT_SLICE_RETRIES, mrSettings.getMaxSliceRetries());
    assertEquals(DEFAULT_SLICE_TIMEOUT_RATIO, mrSettings.getSliceTimeoutRatio());
  }

  public void testNonDefaultSettings() {
    MapSettings.Builder builder = new MapSettings.Builder();
    builder.setBackend("b1");
    try {
      builder.setModule("m").build();
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
      builder.setModule(null);
    }
    builder.setWorkerQueueName("queue1");
    builder.setBaseUrl("base-url");
    builder.setMillisPerSlice(10);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder.setSliceTimeoutRatio(1.5);
    try {
      builder.setSliceTimeoutRatio(0.8);
    } catch (IllegalArgumentException ex) {
      //expected
    }
    builder.setMaxShardRetries(1);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder.setMaxSliceRetries(0);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    MapSettings settings = builder.build();
    assertNull(settings.getModule());
    assertEquals("b1", settings.getBackend());
    assertEquals("queue1", settings.getWorkerQueueName());
    assertEquals("base-url", settings.getBaseUrl());
    assertEquals(10, settings.getMillisPerSlice());
    assertEquals(1, settings.getMaxShardRetries());
    assertEquals(0, settings.getMaxSliceRetries());
    builder.setModule("m1");
    try {
      builder.build();
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
      builder.setBackend(null);
    }
    settings = builder.build();
    assertNull(settings.getBackend());
    assertEquals("m1", settings.getModule());
  }

  public void testValidate() throws Exception {
    MapSettings.Builder builder = new MapSettings.Builder();
    // TODO(user): replace "bad_queue" with "bad-queue". The latter is just
    // an invalid name and does not check if queue exists. see b/13910616
    builder.setWorkerQueueName("bad_queue");
    try {
      builder.build();
      fail("was expecting failure due to bad queue");
    } catch (RuntimeException ex) {
      // expected.
    }
  }

  public void testBuilderWithSettings() {
    MapSettings settings = new MapSettings.Builder()
        .setModule("m")
        .setBaseUrl("url")
        .setMaxShardRetries(10)
        .setMaxSliceRetries(20)
        .setMillisPerSlice(30)
        .setWorkerQueueName("good-queue")
        .build();
    settings = new MapSettings.Builder(settings).build();
    assertNull(settings.getBackend());
    assertEquals("m", settings.getModule());
    assertEquals("url", settings.getBaseUrl());
    assertEquals(10, settings.getMaxShardRetries());
    assertEquals(20, settings.getMaxSliceRetries());
    assertEquals(30, settings.getMillisPerSlice());
    assertEquals("good-queue", settings.getWorkerQueueName());
  }

  public void testMakeShardedJobSettings() {
    Key key = KeyFactory.createKey("Kind1", "value1");
    MapSettings settings = new MapSettings.Builder().setWorkerQueueName("good-queue").build();
    ShardedJobSettings sjSettings = settings.toShardedJobSettings("job1", key);
    assertNull(sjSettings.getBackend());
    assertEquals("default", sjSettings.getModule());
    assertEquals("1", sjSettings.getVersion());
    assertEquals("1.default.test.localhost", sjSettings.getTaskQueueTarget());
    assertEquals(settings.getWorkerQueueName(), sjSettings.getQueueName());
    assertEquals(getPath(settings, "job1", CONTROLLER_PATH), sjSettings.getControllerPath());
    assertEquals(getPath(settings, "job1", WORKER_PATH), sjSettings.getWorkerPath());
    assertEquals(makeViewerUrl(key, key), sjSettings.getPipelineStatusUrl());
    assertEquals(settings.getMaxShardRetries(), sjSettings.getMaxShardRetries());
    assertEquals(settings.getMaxSliceRetries(), sjSettings.getMaxSliceRetries());

    settings = new MapSettings.Builder(settings).setModule(null).setBackend("b1").build();
    sjSettings = settings.toShardedJobSettings("job1", key);
    assertEquals("backend-hostname", sjSettings.getTaskQueueTarget());
    assertEquals("b1", sjSettings.getBackend());
    assertNull(sjSettings.getModule());
    assertNull(sjSettings.getVersion());

    settings = new MapSettings.Builder(settings).setBackend(null).setModule("module1").build();
    sjSettings = settings.toShardedJobSettings("job1", key);
    assertNull(sjSettings.getBackend());
    assertEquals("module1", sjSettings.getModule());
    assertEquals("v1", sjSettings.getVersion());

    settings = new MapSettings.Builder(settings).setModule("default").build();
    Environment env = ApiProxy.getCurrentEnvironment();
    Environment mockEnv = EasyMock.createNiceMock(Environment.class);
    EasyMock.expect(mockEnv.getModuleId()).andReturn("default").atLeastOnce();
    EasyMock.expect(mockEnv.getVersionId()).andReturn("2").atLeastOnce();
    EasyMock.expect(mockEnv.getAttributes()).andReturn(env.getAttributes()).anyTimes();
    EasyMock.replay(mockEnv);
    ApiProxy.setEnvironmentForCurrentThread(mockEnv);
    // Test when current module is the same as requested module
    try {
      sjSettings = settings.toShardedJobSettings("job1", key);
      assertNull(sjSettings.getBackend());
      assertEquals("default", sjSettings.getModule());
      assertEquals("2", sjSettings.getVersion());
    } finally {
      ApiProxy.setEnvironmentForCurrentThread(env);
    }
    EasyMock.verify(mockEnv);
  }

  private String getPath(MapSettings settings, String jobId, String logicPath) {
    return settings.getBaseUrl() + logicPath + "/" + jobId;
  }

  public void testPipelineSettings() {
    MapSettings mrSettings = new MapSettings.Builder().setWorkerQueueName("queue1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(),
        new BackendValidator(null), new ModuleValidator(null), new QueueValidator("queue1"));
    mrSettings = new MapSettings.Builder().setBackend("backend1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(),
        new BackendValidator("backend1"), new ModuleValidator(null), new QueueValidator(null));
    mrSettings = new MapSettings.Builder().setModule("m1").build();
    verifyPipelineSettings(mrSettings.toJobSettings(new StatusConsoleUrl("u1")),
        new BackendValidator(null), new ModuleValidator("m1"),
        new QueueValidator(null), new StatusConsoleValidator("u1"));
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
}
