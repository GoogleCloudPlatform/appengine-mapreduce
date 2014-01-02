// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * Settings that affect how a MapReduce is executed.  May affect performance and
 * resource usage, but should not affect the result (unless the result is
 * dependent on the performance or resource usage of the computation, or if
 * different backends, modules or different base urls have different versions of the code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceSettings implements Serializable {

  private static final long serialVersionUID = 610088354289299175L;

  public static final String DEFAULT_BASE_URL = "/mapreduce/";
  public static final int DEFAULT_MILLIS_PER_SLICE = 10000;
  public static final int DEFAULT_SHARD_RETREIES = 4;
  public static final int DEFAULT_SLICE_RETREIES = 20;

  private String baseUrl = DEFAULT_BASE_URL;
  private String backend;
  private String module;
  private String controllerQueueName = "default";
  private String workerQueueName = "default";
  private String bucketName;
  private int millisPerSlice = DEFAULT_MILLIS_PER_SLICE;
  private int maxShardRetries = DEFAULT_SHARD_RETREIES;
  private int maxSliceRetries = DEFAULT_SLICE_RETREIES;

  public String getBaseUrl() {
    return baseUrl;
  }

  public MapReduceSettings setBaseUrl(String baseUrl) {
    this.baseUrl = checkNotNull(baseUrl, "Null baseUrl");
    return this;
  }

  /*Nullable*/ public String getModule() {
    return module;
  }

  public MapReduceSettings setModule(/*Nullable*/ String module) {
    Preconditions.checkArgument(
        module == null || backend == null, "Module and Backend cannot be combined");
    this.module = module;
    return this;
  }

  /*Nullable*/ public String getBackend() {
    return backend;
  }

  public MapReduceSettings setBackend(/*Nullable*/ String backend) {
    Preconditions.checkArgument(
        module == null || backend == null, "Module and Backend cannot be combined");
    this.backend = backend;
    return this;
  }

  /**
   * @deprecated Controller queue is not used.
   */
  @Deprecated
  public String getControllerQueueName() {
    return controllerQueueName;
  }

  /**
   * @deprecated Controller queue is not used.
   */
  @Deprecated
  public MapReduceSettings setControllerQueueName(String controllerQueueName) {
    this.controllerQueueName = checkNotNull(controllerQueueName, "Null controllerQueueName");
    return this;
  }

  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public MapReduceSettings setWorkerQueueName(String workerQueueName) {
    this.workerQueueName = checkNotNull(workerQueueName, "Null workerQueueName");
    return this;
  }

  /*Nullable*/ public String getBucketName() {
    return bucketName;
  }

  public MapReduceSettings setBucketName(/*Nullable*/ String bucketName) {
    this.bucketName = bucketName;
    return this;
  }

  public int getMillisPerSlice() {
    return millisPerSlice;
  }

  public MapReduceSettings setMillisPerSlice(int millisPerSlice) {
    Preconditions.checkArgument(millisPerSlice >= 0);
    this.millisPerSlice = millisPerSlice;
    return this;
  }

  public int getMaxShardRetries() {
    return maxShardRetries;
  }

  public MapReduceSettings setMaxShardRetries(int maxShardRetries) {
    Preconditions.checkArgument(maxShardRetries >= 0);
    this.maxShardRetries = maxShardRetries;
    return this;
  }

  public int getMaxSliceRetries() {
    return maxSliceRetries;
  }

  public MapReduceSettings setMaxSliceRetries(int maxSliceRetries) {
    Preconditions.checkArgument(maxShardRetries >= 0);
    this.maxSliceRetries = maxSliceRetries;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + baseUrl + ", "
        + backend + ", "
        + module + ", "
        + controllerQueueName + ", "
        + workerQueueName + ", "
        + bucketName + ", "
        + millisPerSlice + ", "
        + maxSliceRetries + ", "
        + maxShardRetries + ")";
  }
}
