// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;

/**
 * Settings that affect how a MapReduce is executed.  May affect performance and
 * resource usage, but should not affect the result (unless the result is
 * dependent on the performance or resource usage of the computation, or if
 * different backends or different base urls have different versions of the
 * code).
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class MapReduceSettings implements Serializable {
  private static final long serialVersionUID = 610088354289299175L;

  public static final String DEFAULT_BASE_URL = "/mapreduce/";

  private String baseUrl = DEFAULT_BASE_URL;
  /*Nullable*/ private String backend = null;
  private String controllerQueueName = "default";
  private String workerQueueName = "default";
  private int millisPerSlice = 10000;

  public MapReduceSettings() {
  }

  public String getBaseUrl() {
    return baseUrl;
  }

  public MapReduceSettings setBaseUrl(String baseUrl) {
    this.baseUrl = checkNotNull(baseUrl, "Null baseUrl");
    return this;
  }

  /*Nullable*/ public String getBackend() {
    return backend;
  }

  public MapReduceSettings setBackend(/*Nullable*/ String backend) {
    this.backend = backend;
    return this;
  }

  public String getControllerQueueName() {
    return controllerQueueName;
  }

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

  public int getMillisPerSlice() {
    return millisPerSlice;
  }

  public MapReduceSettings setMillisPerSlice(int millisPerSlice) {
    this.millisPerSlice = millisPerSlice;
    return this;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + baseUrl + ", "
        + backend + ", "
        + controllerQueueName + ", "
        + workerQueueName + ", "
        + millisPerSlice
        + ")";
  }

}
