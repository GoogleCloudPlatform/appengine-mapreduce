package com.google.appengine.tools.mapreduce.impl.handlers;

import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Limits the number of parallel requests according to their estimated memory usage.
 * Estimates are specified in MB.
 *
 * For internal use only. User code cannot safely depend on this class.
 *
 */
public final class MemoryLimiter {

  private static final Logger log = Logger.getLogger(MemoryLimiter.class.getName());

  private final int INITIAL_SIZE_MB = Ints.saturatedCast(
      (Runtime.getRuntime().maxMemory() - MapReduceConstants.ASSUMED_JVM_RAM_OVERHEAD) / 1024
      / 1024);
  private final Semaphore AMOUNT_REMAINING = new Semaphore(INITIAL_SIZE_MB, true);

  private final int TIME_TO_WAIT = 5000;

  public MemoryLimiter() {}

  private int capRequestedSize(long requested) {
    return (int) Math.min(INITIAL_SIZE_MB, requested);
  }

  /**
   * This method attempts to claim ram to the provided request. This may block waiting for some to
   * be available. Ultimately it is either granted memory or an exception is thrown.
   *
   * @param toClaimMb The amount of memory the request wishes to claim. (In Megabytes)
   * @return The amount of memory which was claimed. (This may be different from the amount
   *         requested.) This value needs to be passed to {@link #release} when the request exits.
   *
   * @throws RejectRequestException If the request should be rejected because it could not be given
   *         the resources requested.
   */
  public long claim(long toClaimMb) throws RejectRequestException {
    Preconditions.checkArgument(toClaimMb >= 0);
    if (toClaimMb == 0) {
      return 0;
    }
    int neededForRequest = capRequestedSize(toClaimMb);
    boolean acquired = false;
    try {
      acquired = AMOUNT_REMAINING.tryAcquire(neededForRequest, TIME_TO_WAIT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RejectRequestException("Was interupted", e);
    }
    int remaining = AMOUNT_REMAINING.availablePermits();
    if (acquired) {
      log.info("Target available memory was " + (neededForRequest + remaining) + "mb is now "
          + remaining + "mb");
      return neededForRequest;
    } else {
      throw new RejectRequestException("Not enough estimated memory for request: "
          + (neededForRequest) + "mb only have " + remaining + "mb remaining out of "
          + INITIAL_SIZE_MB + "mb");
    }
  }

  /**
   * @param ammountUsed the number returned from {@link #claim} when the request began. (Note that
   *        this is NOT the value that was passed to claim.)
   */
  public void release(long ammountUsed) {
    Preconditions.checkArgument(ammountUsed < Integer.MAX_VALUE && ammountUsed >= 0);
    if (ammountUsed == 0) {
      return;
    }
    int toRelease = (int) ammountUsed;
    AMOUNT_REMAINING.release(toRelease);
    int remaining = AMOUNT_REMAINING.availablePermits();
    log.info(
        "Target available memory was " + (remaining - toRelease) + "mb is now " + remaining + "mb");
  }

}
