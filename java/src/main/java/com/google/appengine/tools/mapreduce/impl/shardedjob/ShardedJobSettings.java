// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.labs.modules.ModulesServiceFactory;

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
  // before the rename/removal and make all other fields final.
  private String controllerBackend;
  private String workerBackend;
  private String controllerQueueName;
  private String workerQueueName;

  /*Nullable*/ private String backend;
  /*Nullable*/ private final String module;
  /*Nullable*/ private final String version;
  // TODO(ohler): Integrate with pipeline and put this under /_ah/pipeline.
  /*Nullable*/ private final String pipelineStatusUrl;
  private final String controllerPath;
  private final String workerPath;
  private String target;
  private String queueName;
  private final int maxShardRetries;
  private final int maxSliceRetries;

  /**
   * ShardedJobSettings builder.
   */
  public static class Builder {

    private String backend;
    private String module;
    private String version;
    private String pipelineStatusUrl;
    private String controllerPath = "/mapreduce/controllerCallback";
    private String workerPath = "/mapreduce/workerCallback";
    private String queueName = "default";
    private int maxShardRetries = 4;
    private int maxSliceRetries = 20;

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

    public ShardedJobSettings build() {
      return new ShardedJobSettings(controllerPath, workerPath, pipelineStatusUrl, backend, module,
          version, queueName, maxShardRetries, maxSliceRetries);
    }
  }

  private ShardedJobSettings(String controllerPath, String workerPath, String pipelineStatusUrl,
      String backend, String module, String version, String queueName, int maxShardRetries,
      int maxSliceRetries) {
    this.controllerPath = controllerPath;
    this.workerPath = workerPath;
    this.pipelineStatusUrl = pipelineStatusUrl;
    this.backend = backend;
    this.module = module;
    this.version = version;
    this.queueName = queueName;
    this.maxShardRetries = maxShardRetries;
    this.maxSliceRetries = maxSliceRetries;
    target = resolveTaskQueueTarget();
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
    if (target == null) {
      target = resolveTaskQueueTarget();
    }

    controllerBackend = null;
    workerBackend = null;
    controllerQueueName = null;
    workerQueueName = null;
  }

  private String resolveTaskQueueTarget() {
    if (backend != null) {
      return BackendServiceFactory.getBackendService().getBackendAddress(backend);
    }
    // TODO(user): change to getVersionHostname when 1.8.9 is released
    return ModulesServiceFactory.getModulesService().getModuleHostname(module, version);
  }

  public String getTaskQueueTarget() {
    return target;
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
        + version + ", "
        + target + ")";
  }
}
