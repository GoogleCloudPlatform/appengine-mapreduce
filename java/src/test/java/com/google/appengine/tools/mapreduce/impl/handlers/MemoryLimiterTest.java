package com.google.appengine.tools.mapreduce.impl.handlers;

import junit.framework.TestCase;

/**
 * Tests MemoryLimiter
 *
 */
public class MemoryLimiterTest extends TestCase {

  public void testZero() {
    MemoryLimiter limiter = new MemoryLimiter();
    long claimed = limiter.claim(0);
    assertEquals(0, claimed);
    limiter.release(claimed);
  }

  public void testRequestsGoThrough() {
    MemoryLimiter limiter = new MemoryLimiter();
    long claimed = limiter.claim(10);
    assertEquals(10, claimed);
    limiter.release(claimed);
  }

  public void testAboveMaxAllocation() {
    MemoryLimiter limiter = new MemoryLimiter();
    long claimed = limiter.claim(Integer.MAX_VALUE);
    assertTrue(claimed > 0);
    assertTrue(claimed < Integer.MAX_VALUE);
    try {
      limiter.claim(Integer.MAX_VALUE);
      fail();
    } catch (RejectRequestException e) {
      //expected
    }
    limiter.release(claimed);
    claimed = limiter.claim(Integer.MAX_VALUE);
    limiter.release(claimed);
  }

  public void testSmallBehindLargeOne() {
    MemoryLimiter limiter = new MemoryLimiter();
    long mediumClaimed = limiter.claim(10);
    try {
      limiter.claim(Integer.MAX_VALUE);
      fail();
    } catch (RejectRequestException e) {
      // Expected
    }
    long smallClaimed = limiter.claim(1);
    limiter.release(mediumClaimed);
    try {
      limiter.claim(Integer.MAX_VALUE);
      fail();
    } catch (RejectRequestException e) {
      // Expected
    }
    limiter.release(smallClaimed);
    long largeClaimed = limiter.claim(Integer.MAX_VALUE);
    try {
      limiter.claim(1);
      fail();
    } catch (RejectRequestException e) {
      // Expected
    }
    limiter.release(largeClaimed);
  }

}
