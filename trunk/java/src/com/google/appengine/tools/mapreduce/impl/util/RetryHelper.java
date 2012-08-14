// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Utility class for retrying operations.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <V> return value of the closure that is being run with retries
 */
public class RetryHelper<V> {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(RetryHelper.class.getName());

  // TODO(ohler): move into a RetryParameters/RetryStrategy object
  private static final int RETRY_MIN_ATTEMPTS = 5;
  private static final int RETRY_MAX_ATTEMPTS = 200;
  private static final long INITIAL_RETRY_DELAY_MILLIS = 10;
  private static final long MAX_RETRY_DELAY_MILLIS = 10 * 1000;
  private static final double RETRY_DELAY_BACKOFF_FACTOR = 2;
  private static final long RETRY_PERIOD_MILLIS = 4 * 60 * 1000;

  public static String messageChain(Throwable t) {
    StringBuilder b = new StringBuilder("" + t);
    t = t.getCause();
    while (t != null) {
      b.append("\n -- caused by: " + t);
      t = t.getCause();
    }
    return "" + b;
  }

  /** Body to be run and retried if it doesn't succeed. */
  public interface Body<V> {
    V run() throws IOException;
  }

  private final Stopwatch stopwatch = new Stopwatch();
  private int attemptsSoFar = 0;
  private final Body<V> body;

  private RetryHelper(Body<V> body) {
    this.body = body;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "(" + stopwatch + ", "
        + attemptsSoFar + " attempts, " + body + ")";
  }

  private V doRetry() {
    stopwatch.start();
    while (true) {
      attemptsSoFar++;
      Exception exception;
      try {
        V value = body.run();
        if (attemptsSoFar > 1) {
          log.info(this + ": retry successful");
        }
        return value;
      } catch (IOException e) {
        exception = e;
      } catch (ApiProxyException e) {
        // TODO(ohler): retry only the subclasses that make sense, such as
        // ApiDeadlineExceededException.  (Which should arguably be wrapped in
        // an IOException for calls like open()...)
        exception = e;
      }
      long sleepDurationMillis =
          (long) (Math.random()
              * (Math.min(MAX_RETRY_DELAY_MILLIS,
                      Math.pow(RETRY_DELAY_BACKOFF_FACTOR, attemptsSoFar - 1)
                          * INITIAL_RETRY_DELAY_MILLIS)));
      log.warning(this + ": Attempt " + attemptsSoFar + " failed, sleeping for "
          + sleepDurationMillis + " ms: " + messageChain(exception));
      if (attemptsSoFar >= RETRY_MAX_ATTEMPTS
          || (attemptsSoFar >= RETRY_MIN_ATTEMPTS
              && stopwatch.elapsedMillis() >= RETRY_PERIOD_MILLIS)) {
        throw new RuntimeException(this + ": Too many failures, giving up", exception);
      }
      try {
        Thread.sleep(sleepDurationMillis);
      } catch (InterruptedException e2) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(
            this + ": Unexpected interruption while retrying after " + exception, e2);
      }
    }
  }

  public static <V> V runWithRetries(Body<V> body) {
    return new RetryHelper<V>(body).doRetry();
  }

}
