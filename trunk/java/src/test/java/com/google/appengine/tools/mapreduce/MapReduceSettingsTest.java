// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_MILLIS_PER_SLICE;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SHARD_RETREIES;
import static com.google.appengine.tools.mapreduce.MapSettings.DEFAULT_SLICE_RETREIES;
import static junit.framework.Assert.assertNull;

import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

import junit.framework.TestCase;

/**
 */
@SuppressWarnings("deprecation")
public class MapReduceSettingsTest extends TestCase {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper();

  @Override
  public void setUp() {
    helper.setUp();
  }

  public void testDefaultSettings() {
    MapReduceSettings mrSettings = new MapReduceSettings.Builder().build();
    assertNull(mrSettings.getBackend());
    assertNull(mrSettings.getModule());
    assertEquals("app_default_bucket", mrSettings.getBucketName());
    assertNull(mrSettings.getWorkerQueueName());
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals(DEFAULT_MILLIS_PER_SLICE, mrSettings.getMillisPerSlice());
    assertEquals(DEFAULT_SHARD_RETREIES, mrSettings.getMaxShardRetries());
    assertEquals(DEFAULT_SLICE_RETREIES, mrSettings.getMaxSliceRetries());
    assertNull(mrSettings.getMaxSortMemory());
  }

  public void testNonDefaultSettings() {
    MapReduceSettings.Builder builder = new MapReduceSettings.Builder();
    builder.setBackend("b1");
    try {
      builder.setModule("m").build();
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
      builder.setModule(null);
    }
    builder = builder.setBucketName("bucket");
    builder = builder.setWorkerQueueName("queue1");
    builder = builder.setBaseUrl("base-url");
    builder = builder.setMillisPerSlice(10);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxShardRetries(1);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxSliceRetries(0);
    try {
      builder.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    builder = builder.setMaxSortMemory(10L);
    try {
      builder.setMaxSortMemory(-1L);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    MapReduceSettings mrSettings = builder.build();
    assertNull(mrSettings.getModule());
    assertEquals("b1", mrSettings.getBackend());
    assertEquals("bucket", mrSettings.getBucketName());
    assertEquals("queue1", mrSettings.getWorkerQueueName());
    assertEquals("base-url", mrSettings.getBaseUrl());
    assertEquals(10, mrSettings.getMillisPerSlice());
    assertEquals(1, mrSettings.getMaxShardRetries());
    assertEquals(0, mrSettings.getMaxSliceRetries());
    assertEquals(10L, (long) mrSettings.getMaxSortMemory());
    builder = new MapReduceSettings.Builder().setModule("m1");
    try {
      builder.setBackend("b").build();
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
      builder.setBackend(null);
    }
    mrSettings = builder.build();
    assertNull(mrSettings.getBackend());
    assertEquals("m1", mrSettings.getModule());
  }
}
