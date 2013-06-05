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

  @Override public String toString() {
    return getClass().getSimpleName() + "("
        + controllerBackend + ", "
        + workerBackend + ", "
        + controllerPath + ", "
        + workerPath + ", "
        + controllerQueueName + ", "
        + workerQueueName + ", "
        + millisBetweenPolls
        + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((controllerBackend == null) ? 0 : controllerBackend.hashCode());
    result = prime * result + ((controllerPath == null) ? 0 : controllerPath.hashCode());
    result = prime * result + ((controllerQueueName == null) ? 0 : controllerQueueName.hashCode());
    result = prime * result + millisBetweenPolls;
    result = prime * result + ((workerBackend == null) ? 0 : workerBackend.hashCode());
    result = prime * result + ((workerPath == null) ? 0 : workerPath.hashCode());
    result = prime * result + ((workerQueueName == null) ? 0 : workerQueueName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ShardedJobSettings other = (ShardedJobSettings) obj;
    if (controllerBackend == null) {
      if (other.controllerBackend != null) {
        return false;
      }
    } else if (!controllerBackend.equals(other.controllerBackend)) {
      return false;
    }
    if (controllerPath == null) {
      if (other.controllerPath != null) {
        return false;
      }
    } else if (!controllerPath.equals(other.controllerPath)) {
      return false;
    }
    if (controllerQueueName == null) {
      if (other.controllerQueueName != null) {
        return false;
      }
    } else if (!controllerQueueName.equals(other.controllerQueueName)) {
      return false;
    }
    if (millisBetweenPolls != other.millisBetweenPolls) {
      return false;
    }
    if (workerBackend == null) {
      if (other.workerBackend != null) {
        return false;
      }
    } else if (!workerBackend.equals(other.workerBackend)) {
      return false;
    }
    if (workerPath == null) {
      if (other.workerPath != null) {
        return false;
      }
    } else if (!workerPath.equals(other.workerPath)) {
      return false;
    }
    if (workerQueueName == null) {
      if (other.workerQueueName != null) {
        return false;
      }
    } else if (!workerQueueName.equals(other.workerQueueName)) {
      return false;
    }
    return true;
  }

}
