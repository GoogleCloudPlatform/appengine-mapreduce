// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.modules.ModulesServiceFactory;

import java.io.IOException;
import java.io.Serializable;

/**
 * Execution settings for a sharded job.
 *
 * @author ohler@google.com (Christian Ohler)
 */
public final class ShardedJobSettings implements Serializable {

  private static final long serialVersionUID = 286995366653078363L;

  public static final int DEFAULT_SLICE_TIMEOUT_MILLIS = 11 * 60000;

  /*Nullable*/ private final String backend;
  /*Nullable*/ private final String module;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;
  /*Nullable*/ private final String mrStatusUrl;
  private final String controllerPath;
  private final String workerPath;
  private final String target;
  private final String queueName;
  private final int maxShardRetries;
  private final int maxSliceRetries;
  private final int sliceTimeoutMillis;

  /**
   * ShardedJobSettings builder.
   */
  public static class Builder {

    private String backend;
    private String module;
    private String version;
    private String pipelineStatusUrl;
    private String mrStatusUrl;
    private String controllerPath = "/mapreduce/controllerCallback";
    private String workerPath = "/mapreduce/workerCallback";
    private String queueName = "default";
    private int maxShardRetries = 4;
    private int maxSliceRetries = 20;
    private int sliceTimeoutMillis = DEFAULT_SLICE_TIMEOUT_MILLIS;

    public Builder setPipelineStatusUrl(String pipelineStatusUrl) {
      this.pipelineStatusUrl = pipelineStatusUrl;
      return this;
    }

    public Builder setBackend(/*Nullable*/ String backend) {
      this.backend = backend;
      return this;
    }

    public Builder setModule(String module) {
      this.module = module;
      return this;
    }

    public Builder setVersion(String version) {
      this.version = version;
      return this;
    }

    public Builder setControllerPath(String controllerPath) {
      this.controllerPath = checkNotNull(controllerPath, "Null controllerPath");
      return this;
    }

    public Builder setWorkerPath(String workerPath) {
      this.workerPath = checkNotNull(workerPath, "Null workerPath");
      return this;
    }

    public Builder setQueueName(String queueName) {
      this.queueName = checkNotNull(queueName, "Null queueName");
      return this;
    }

    public Builder setMaxShardRetries(int maxShardRetries) {
      this.maxShardRetries = maxShardRetries;
      return this;
    }

    public Builder setMaxSliceRetries(int maxSliceRetries) {
      this.maxSliceRetries = maxSliceRetries;
      return this;
    }

    public Builder setSliceTimeoutMillis(int sliceTimeoutMillis) {
      this.sliceTimeoutMillis = sliceTimeoutMillis;
      return this;
    }

    public Builder setMapReduceStatusUrl(String mrStatusUrl) {
      this.mrStatusUrl = mrStatusUrl;
      return this;
    }

    public ShardedJobSettings build() {
      return new ShardedJobSettings(controllerPath, workerPath, mrStatusUrl, pipelineStatusUrl,
          backend, module, version, queueName, maxShardRetries, maxSliceRetries,
          sliceTimeoutMillis);
    }
  }

  private ShardedJobSettings(String controllerPath, String workerPath, String mrStatusUrl,
      String pipelineStatusUrl, String backend, String module, String version, String queueName,
      int maxShardRetries, int maxSliceRetries, int sliceTimeoutMillis) {
    this.controllerPath = controllerPath;
    this.workerPath = workerPath;
    this.mrStatusUrl = mrStatusUrl;
    this.pipelineStatusUrl = pipelineStatusUrl;
    this.backend = backend;
    this.module = module;
    this.version = version;
    this.queueName = queueName;
    this.maxShardRetries = maxShardRetries;
    this.maxSliceRetries = maxSliceRetries;
    this.sliceTimeoutMillis = sliceTimeoutMillis;
    target = resolveTaskQueueTarget();
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  private String resolveTaskQueueTarget() {
    if (backend != null) {
      return BackendServiceFactory.getBackendService().getBackendAddress(backend);
    }
    return ModulesServiceFactory.getModulesService().getVersionHostname(module, version);
  }

  public String getTaskQueueTarget() {
    return target;
  }

  /*Nullable*/ public String getMapReduceStatusUrl() {
    return mrStatusUrl;
  }

  /*Nullable*/ public String getPipelineStatusUrl() {
    return pipelineStatusUrl;
  }

  /*Nullable*/ public String getBackend() {
    return backend;
  }

  public String getModule() {
    return module;
  }

  public String getVersion() {
    return version;
  }

  public String getControllerPath() {
    return controllerPath;
  }

  public String getWorkerPath() {
    return workerPath;
  }

  public String getQueueName() {
    return queueName;
  }

  public int getMaxShardRetries() {
    return maxShardRetries;
  }

  public int getMaxSliceRetries() {
    return maxSliceRetries;
  }

  public int getSliceTimeoutMillis() {
    return sliceTimeoutMillis;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "("
        + backend + ", "
        + mrStatusUrl + ", "
        + pipelineStatusUrl + ", "
        + controllerPath + ", "
        + workerPath + ", "
        + queueName + ", "
        + maxShardRetries + ", "
        + maxSliceRetries + ", "
        + module + ", "
        + version + ", "
        + target + ", "
        + sliceTimeoutMillis + ")";
  }
}
