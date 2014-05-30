// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_BASE_URL;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_MILLIS_PER_SLICE;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_SHARD_RETREIES;
import static com.google.appengine.tools.mapreduce.MapReduceSettings.DEFAULT_SLICE_RETREIES;

import junit.framework.TestCase;

/**
 */
@SuppressWarnings("deprecation")
public class MapReduceSettingsTest extends TestCase {

  public void testDefaultSettings() {
    MapReduceSettings mrSettings = new MapReduceSettings();
    assertNull(mrSettings.getBackend());
    assertNull(mrSettings.getModule());
    assertNull(mrSettings.getBucketName());
    assertNull(mrSettings.getWorkerQueueName());
    assertEquals(DEFAULT_BASE_URL, mrSettings.getBaseUrl());
    assertEquals(DEFAULT_MILLIS_PER_SLICE, mrSettings.getMillisPerSlice());
    assertEquals(DEFAULT_SHARD_RETREIES, mrSettings.getMaxShardRetries());
    assertEquals(DEFAULT_SLICE_RETREIES, mrSettings.getMaxSliceRetries());
  }

  public void testNonDefaultSettings() {
    MapReduceSettings mrSettings = new MapReduceSettings();
    mrSettings.setBackend("b1");
    try {
      mrSettings.setModule("m");
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    mrSettings.setBucketName("bucket");
    mrSettings.setWorkerQueueName("queue1");
    mrSettings.setBaseUrl("base-url");
    mrSettings.setMillisPerSlice(10);
    try {
      mrSettings.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    mrSettings.setMaxShardRetries(1);
    try {
      mrSettings.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    mrSettings.setMaxSliceRetries(0);
    try {
      mrSettings.setMillisPerSlice(-1);
    } catch (IllegalArgumentException ex) {
      // expected
    }
    assertNull(mrSettings.getModule());
    assertEquals("b1", mrSettings.getBackend());
    assertEquals("bucket", mrSettings.getBucketName());
    assertEquals("queue1", mrSettings.getWorkerQueueName());
    assertEquals("base-url", mrSettings.getBaseUrl());
    assertEquals(10, mrSettings.getMillisPerSlice());
    assertEquals(1, mrSettings.getMaxShardRetries());
    assertEquals(0, mrSettings.getMaxSliceRetries());
    mrSettings.setBackend(null);
    mrSettings.setModule("m1");
    try {
      mrSettings.setBackend("b");
      fail("Expected exception to be thrown");
    } catch (IllegalArgumentException ex) {
      // expected
    }
    assertNull(mrSettings.getBackend());
    assertEquals("m1", mrSettings.getModule());
  }
}
