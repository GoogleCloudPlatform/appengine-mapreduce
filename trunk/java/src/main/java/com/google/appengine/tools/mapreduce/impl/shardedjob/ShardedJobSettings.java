// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public final class ShardedJobSettings implements Serializable {

  private static final long serialVersionUID = 286995366653078363L;

  /*Nullable*/ private String controllerBackend = null;
  /*Nullable*/ private String workerBackend = null;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  private String controllerPath = "/mapreduce/controllerCallback";
  private String workerPath = "/mapreduce/workerCallback";
  private String controllerQueueName = "default";
  private String workerQueueName = "default";
  private int millisBetweenPolls = 2000;
  private int maxShardRetries = 4;

  public ShardedJobSettings() {
  }

  /*Nullable*/ public String getControllerBackend() {
    return controllerBackend;
  }

  public ShardedJobSettings setControllerBackend(/*Nullable*/ String controllerBackend) {
    this.controllerBackend = controllerBackend;
    return this;
  }

  /*Nullable*/ public String getWorkerBackend() {
    return workerBackend;
  }

  public ShardedJobSettings setWorkerBackend(/*Nullable*/ String workerBackend) {
    this.workerBackend = workerBackend;
    return this;
  }

  public String getControllerPath() {
    return controllerPath;
  }

  public ShardedJobSettings setControllerPath(String controllerPath) {
    this.controllerPath = checkNotNull(controllerPath, "Null controllerPath");
    return this;
  }

  public String getWorkerPath() {
    return workerPath;
  }

  public ShardedJobSettings setWorkerPath(String workerPath) {
    this.workerPath = checkNotNull(workerPath, "Null workerPath");
    return this;
  }

  public String getControllerQueueName() {
    return controllerQueueName;
  }

  public ShardedJobSettings setControllerQueueName(String controllerQueueName) {
    this.controllerQueueName = checkNotNull(controllerQueueName, "Null controllerQueueName");
    return this;
  }

  public String getWorkerQueueName() {
    return workerQueueName;
  }

  public ShardedJobSettings setWorkerQueueName(String workerQueueName) {
    this.workerQueueName = checkNotNull(workerQueueName, "Null workerQueueName");
    return this;
  }

  public int getMillisBetweenPolls() {
    return millisBetweenPolls;
  }

  public ShardedJobSettings setMillisBetweenPolls(int millisBetweenPolls) {
    this.millisBetweenPolls = millisBetweenPolls;
    return this;
  }

  public int getMaxShardRetries() {
    return maxShardRetries;
  }

  public void setMaxShardRetries(int maxShardRetries) {
    this.maxShardRetries = maxShardRetries;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + controllerBackend + ", "
        + workerBackend + ", "
        + controllerPath + ", "
        + workerPath + ", "
        + controllerQueueName + ", "
        + workerQueueName + ", "
        + millisBetweenPolls + ","
        + maxShardRetries + ")";
  }
}
