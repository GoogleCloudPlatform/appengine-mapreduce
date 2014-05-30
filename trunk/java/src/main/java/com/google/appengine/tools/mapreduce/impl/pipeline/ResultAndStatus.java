package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;

import java.io.Serializable;

/**
 * A holder for MR result and its status.
 *
 * @param <R> the type of {@code MapReduceResult} content.
 */
public final class ResultAndStatus<R> implements Serializable {

  private static final long serialVersionUID = 7867829838406777714L;

  private final MapReduceResult<R> result;
  private final Status status;

  public ResultAndStatus(MapReduceResult<R> result, Status status) {
    this.result = result;
    this.status = status;
  }

  public MapReduceResult<R> getResult() {
    return result;
  }

  public Status getStatus() {
    return status;
  }
}