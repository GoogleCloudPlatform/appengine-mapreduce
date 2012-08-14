// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

/**
 * Possible job statuses.
 *
 */
public enum Status {

  /**
   * Job is initializing.
   */
  INITIALIZING,

  /**
   * Job is running.
   */
  RUNNING,

  /**
   * Job has successfully completed.
   */
  DONE,

  /**
   * Job stopped because of error.
   */
  ERROR,

  /**
   * Job stopped because of user abort request.
   */
  ABORTED;

  public boolean isActive() {
    return this == INITIALIZING || this == RUNNING;
  }

}
