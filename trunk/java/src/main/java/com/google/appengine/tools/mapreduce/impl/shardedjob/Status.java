// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * Possible job statuses.
 *
 */
public class Status implements Serializable {

  private static final long serialVersionUID = 8387217197847622711L;

  /**
   * The possible status codes for {@code Status}.
   */
  public enum StatusCode {
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
  }

  private final StatusCode statusCode;
  private final Exception exception;

  public Status(StatusCode statusCode) {
    this(statusCode, null);
  }

  public Status(StatusCode statusCode, /*Nullable*/ Exception exception) {
    this.statusCode = checkNotNull(statusCode);
    Preconditions.checkArgument(exception == null || statusCode == StatusCode.ERROR,
        "Exception can be provided only when status code is ERROR.");
    this.exception = exception;
  }

  public boolean isActive() {
    return statusCode == StatusCode.INITIALIZING || statusCode == StatusCode.RUNNING;
  }

  public StatusCode getStatusCode() {
    return statusCode;
  }

  public Exception getException() {
    return exception;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(statusCode, exception);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Status other = (Status) obj;
    return Objects.equal(statusCode, other.statusCode) && Objects.equal(exception, other.exception);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + statusCode + ", " + exception + ")";
  }
}
