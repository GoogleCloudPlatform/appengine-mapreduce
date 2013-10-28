package com.google.appengine.tools.mapreduce.impl.handlers;


/**
 * An exception thrown to reject the current request with an error code (50X) This will usually
 * cause taskqueue to retry the request on another instance.
 *
 * For internal use only. User code cannot safely depend on this class.
 */
public class RejectRequestException extends RuntimeException {

  private static final long serialVersionUID = 5938529235133524752L;

  public RejectRequestException(String reason) {
    super(reason);
  }

  public RejectRequestException(String reason, Exception e) {
    super(reason, e);
  }

}
