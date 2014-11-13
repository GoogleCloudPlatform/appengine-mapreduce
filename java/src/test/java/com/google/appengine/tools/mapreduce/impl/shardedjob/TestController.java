package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

/**
 * A mock controller used for unit tests. It simply sums the inputs to combine the results.
 *
 */
public class TestController extends ShardedJobController<TestTask> {

  private static final long serialVersionUID = 1L;
  private final int expectedResult;
  private boolean completed = false;

  public TestController(int expectedResult) {
    this.expectedResult = expectedResult;
  }

  @Override
  public void completed(Iterator<TestTask> results) {
    int sum = 0;
    while (results.hasNext()) {
      sum += results.next().getResult();
    }
    assertEquals(expectedResult, sum);
    assertFalse(completed);
    completed = true;
  }

  @Override
  public void failed(Status status) {
    fail("Should not have been called");
  }

  public boolean isCompleted() {
    return completed;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (completed ? 1231 : 1237);
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
    if (getClass() != obj.getClass()) {
      return false;
    }
    TestController other = (TestController) obj;
    if (completed != other.completed) {
      return false;
    }
    return expectedResult == other.expectedResult;
  }

}
