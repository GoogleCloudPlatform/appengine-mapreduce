package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Test that ConcatenatingInputReader does what it's name implies
 */
public class ConcatenatingInputReaderTest extends TestCase {

  private List<InputReader<Long>> createReaders(int num) {
    ArrayList<InputReader<Long>> result = new ArrayList<InputReader<Long>>(num);
    for (int i = 0; i < num; i++) {
      result.add(new ConsecutiveLongInput.Reader(0, 10));
    }
    return result;
  }

  public void testConcatenates() throws NoSuchElementException, IOException {
    final int numReader = 10;
    ConcatenatingInputReader<Long> cat =
        new ConcatenatingInputReader<Long>(createReaders(numReader));
    for (int i = 0; i < numReader; i++) {
      for (long j = 0; j < 10; j++) {
        assertEquals((Long) j, cat.next());
      }
    }
    try {
      cat.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }

  public void testProgress() throws NoSuchElementException, IOException {
    final int numReader = 10;
    ConcatenatingInputReader<Long> cat =
        new ConcatenatingInputReader<Long>(createReaders(numReader));
    Double progress = cat.getProgress();
    assertEquals(0.0, progress);
    for (int i = 0; i < 10 * numReader; i++) {
      cat.next();
      assertTrue("Progress was " + progress + " is now " + cat.getProgress(),
          progress <= cat.getProgress());
      progress = cat.getProgress();
    }
    assertEquals(1.0, progress);
  }

}
