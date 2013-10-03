package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.inputs.ConcatenatingInputReader.ReaderCreator;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Test that ConcatenatingInputReader does what it's name implies
 */
public class ConcatenatingInputReaderTest extends TestCase {

  private static class TestReaderCreator implements ReaderCreator<Long> {
    final int numReaders;
    int count;

    TestReaderCreator(int numReaders) {
      this.numReaders = numReaders;
      this.count = numReaders;
    }
    @Override
    public InputReader<Long> createNextReader() {
      if (count == 0) {
        return null;
      }
      count--;
      return new ConsecutiveLongInput.Reader(0, 10);
    }

    @Override
    public int estimateTotalNumberOfReaders() {
      return numReaders;
    }
  }
 

  public void testConcatenates() throws NoSuchElementException, IOException {
    final int numReader = 10;
    ConcatenatingInputReader<Long> cat =
        new ConcatenatingInputReader<Long>(new TestReaderCreator(numReader));
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
        new ConcatenatingInputReader<Long>(new TestReaderCreator(numReader));
    Double progress = cat.getProgress();
    assertEquals(0.0, progress);
    for (int i = 0; i < 10 * numReader; i++) {
      cat.next();
      assertTrue("Progress was " + progress + " is now " + cat.getProgress(),
          progress < cat.getProgress());
      progress = cat.getProgress();
    }
    assertEquals(1.0, progress);
  }

}
