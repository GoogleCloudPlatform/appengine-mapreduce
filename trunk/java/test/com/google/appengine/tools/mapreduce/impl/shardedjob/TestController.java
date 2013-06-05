package com.google.appengine.tools.mapreduce.impl.shardedjob;

import junit.framework.Assert;

/**
 * A mock controller used for unit tests. It simply sums the inputs to combine the results.
 *
 */
public class TestController implements ShardedJobController<TestTask, Integer> {

  private static final long serialVersionUID = 1L;
  private final int expectedResult;

  public TestController(int expectedResult) {
    this.expectedResult = expectedResult;
  }

  @Override public Integer combineResults(Iterable<Integer> inputs) {
    int sum = 0;
    for (Integer x : inputs) {
      sum += x;
    }
    return sum;
  }

  // TODO(ohler): Assert that this actually gets called.  Seems likely that
  // will be covered by higher-level end-to-end tests, though.
  @Override public void completed(Integer finalCombinedResult) {
    Assert.assertEquals(Integer.valueOf(expectedResult), finalCombinedResult);
  }
 

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + expectedResult;
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof TestController)) {
      return false;
    }
    TestController other = (TestController) obj;
    if (expectedResult != other.expectedResult) {
      return false;
    }
    return true;
  }
}