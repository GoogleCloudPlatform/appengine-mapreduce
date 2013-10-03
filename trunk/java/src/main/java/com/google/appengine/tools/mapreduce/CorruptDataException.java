package com.google.appengine.tools.mapreduce;

/**
 * The exception generated if any of the data appears to be corrupt. This should cause the MapReduce
 * to fail.
 */
public class CorruptDataException extends RuntimeException {

  private static final long serialVersionUID = 5053922369001406602L;

  public CorruptDataException() {
    super();
  }

  public CorruptDataException(String message) {
    super(message);
  }

  public CorruptDataException(String message, Throwable cause) {
    super(message, cause);
  }

  public CorruptDataException(Throwable cause) {
    super(cause);
  }
}
