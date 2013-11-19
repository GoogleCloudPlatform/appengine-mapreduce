package com.google.appengine.tools.mapreduce.impl;

/**
 * An exception that indicates it is safe to restart a slice.
 */
public class RecoverableException extends RuntimeException {

  private static final long serialVersionUID = -1527377663569164133L;

  public RecoverableException(String message, Throwable rootCause) {
    super(message, rootCause);
  }
}
