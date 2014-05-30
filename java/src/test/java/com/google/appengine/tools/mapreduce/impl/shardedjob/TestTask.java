package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.junit.Assert.assertTrue;

import com.google.appengine.tools.mapreduce.impl.IncrementalTaskContext;
import com.google.appengine.tools.mapreduce.impl.IncrementalTaskWithContext;


/**
 * A simple intermediate tasks object to be used in unit tests.
 *
 */
public class TestTask implements IncrementalTaskWithContext {
  private static final long serialVersionUID = 1L;
  private final IncrementalTaskContext context;
  private final int valueToYield;
  @SuppressWarnings("unused")
  private final byte[] initialPayload;
  private int total = 0;
  private int slicesRemaining;

  public TestTask(int shardId, int shardCount, int valueToYield, int numSlices, byte... payload) {
    this.context =
        new IncrementalTaskContext("TestMR", shardId, shardCount, "testCalls", "testCallsMillis");
    this.valueToYield = valueToYield;
    slicesRemaining = numSlices;
    this.initialPayload = payload;
  }

  @Override
  public void prepare() {
  }

  @Override
  public void run() {
    assertTrue(slicesRemaining-- > 0);
    total += valueToYield;
    context.getCounters().getCounter("TestTaskSum").increment(valueToYield);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean isDone() {
    return slicesRemaining <= 0;
  }

  public Integer getResult() {
    return total;
  }

  @Override
  public IncrementalTaskContext getContext() {
    return context;
  }

  @Override
  public boolean allowSliceRetry(boolean abandon) {
    return false;
  }
}
