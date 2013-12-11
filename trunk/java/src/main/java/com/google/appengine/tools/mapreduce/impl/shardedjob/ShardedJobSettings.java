// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public final class ShardedJobSettings implements Serializable {

  private static final long serialVersionUID = 286995366653078363L;

  // TODO(user): remove fields and readObject once we no longer care about outstanding jobs
  // before the rename/removal.
  private String controllerBackend;
  private String workerBackend;
  private String controllerQueueName;
  private String workerQueueName;

  /*Nullable*/ private String backend;
  /*Nullable*/ private String module;
  /*Nullable*/ private String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private String pipelineStatusUrl;
  private String controllerPath = "/mapreduce/controllerCallback";
  private String workerPath = "/mapreduce/workerCallback";
  private String queueName = "default";
  private int maxShardRetries = 4;
  private int maxSliceRetries = 20;

  public ShardedJobSettings() {
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    if (backend == null) {
      backend = workerBackend == null ? controllerBackend : workerBackend;
    }
    if (queueName == null || queueName.equals("default")) {
      queueName = workerQueueName == null ? controllerQueueName : workerQueueName;
      if (queueName == null) {
        queueName = "default";
      }
    }

    controllerBackend = null;
    workerBackend = null;
    controllerQueueName = null;
    workerQueueName = null;
  }

  /*Nullable*/ public String getPipelineStatusUrl() {
    return pipelineStatusUrl;
  }

  public ShardedJobSettings setPipelineStatusUrl(/*Nullable*/ String pipelineStatusUrl) {
    this.pipelineStatusUrl = pipelineStatusUrl;
    return this;
  }

  /*Nullable*/ public String getBackend() {
    return backend;
  }

  public ShardedJobSettings setBackend(/*Nullable*/ String backend) {
    this.backend = backend;
    return this;
  }

  public String getModule() {
    return module;
  }

  public ShardedJobSettings setModule(String module) {
    this.module = module;
    return this;
  }

  public String getVersion() {
    return version;
  }

  public ShardedJobSettings setVersion(String version) {
    this.version = version;
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

  public String getQueueName() {
    return queueName;
  }

  public ShardedJobSettings setQueueName(String queueName) {
    this.queueName = checkNotNull(queueName, "Null queueName");
    return this;
  }

  public int getMaxShardRetries() {
    return maxShardRetries;
  }

  public ShardedJobSettings setMaxShardRetries(int maxShardRetries) {
    this.maxShardRetries = maxShardRetries;
    return this;
  }

  public int getMaxSliceRetries() {
    return maxSliceRetries;
  }

  public ShardedJobSettings setMaxSliceRetries(int maxSliceRetries) {
    this.maxSliceRetries = maxSliceRetries;
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + backend + ", "
        + pipelineStatusUrl + ", "
        + controllerPath + ", "
        + workerPath + ", "
        + queueName + ", "
        + maxShardRetries + ", "
        + maxSliceRetries + ", "
        + module + ", "
        + version + ")";
  }
}
