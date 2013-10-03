// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

  private static final Logger log = Logger.getLogger(RetryHelper.class.getName());


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

  private final Stopwatch stopwatch = Stopwatch.createUnstarted();
  private int attemptsSoFar = 0;
  private final Body<V> body;
  private final RetryParams params;

  private RetryHelper(Body<V> body, RetryParams parms) {
    this.body = body;
    this.params = parms;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + stopwatch + ", " + attemptsSoFar + " attempts, "
        + body + ")";
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
        // ApiDeadlineExceededException. (Which should arguably be wrapped in
        // an IOException for calls like open()...)
        exception = e;
      }
      long sleepDurationMillis = (long) ((Math.random() / 2.0 + .5) * (Math.min(
          params.getMaxRetryDelayMillis(),
              Math.pow(params.getRetryDelayBackoffFactor(), attemptsSoFar - 1)
              * params.getInitialRetryDelayMillis())));
      log.warning(this + ": Attempt " + attemptsSoFar + " failed, sleeping for "
          + sleepDurationMillis + " ms: " + messageChain(exception));
      if (attemptsSoFar >= params.getRetryMaxAttempts() 
          || (attemptsSoFar >= params.getRetryMinAttempts()
              && stopwatch.elapsed(MILLISECONDS) >= params.getRetryPeriodMillis())) {
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
    return new RetryHelper<V>(body, new RetryParams()).doRetry();
  }

  public static <V> V runWithRetries(Body<V> body, RetryParams parms) {
    return new RetryHelper<V>(body, parms).doRetry();
  }

}
