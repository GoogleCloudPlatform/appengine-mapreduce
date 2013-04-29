package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.InputReader;

import java.util.List;

/**
 * Unit test for {@link CloudStorageLineInput}.
 */
public class CloudStorageLineInputTest extends CloudStorageLineInputTestCase {
  
  private static final String FILENAME = "CloudStorageLineInputTestFile";
  private static final String BUCKET = "CloudStorageLineInputTestBucket";
  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 1000;

  GcsFilename filename = new GcsFilename(BUCKET, FILENAME);
  long fileSize;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    fileSize = createFile(filename, RECORD, RECORDS_COUNT);
  }

  public void testSplit() throws Exception {
    CloudStorageLineInput input = new CloudStorageLineInput(filename, (byte) '\n', 4);
    List<? extends InputReader<byte[]>> readers = input.createReaders();
    assertEquals(4, readers.size());
    assertSplitRange(0, 3000, readers.get(0));
    assertSplitRange(3000, 6000, readers.get(1));
    assertSplitRange(6000, 9000, readers.get(2));
    assertSplitRange(9000, 12000, readers.get(3));
  }

  public void testUnevenSplit() throws Exception {
    CloudStorageLineInput input = new CloudStorageLineInput(filename, (byte) '\n', 7);
    List<? extends InputReader<byte[]>> readers = input.createReaders();
    assertEquals(7, readers.size());
    assertSplitRange(0, 1714, readers.get(0));
    assertSplitRange(1714, 3428, readers.get(1));
    assertSplitRange(3428, 5142, readers.get(2));
    assertSplitRange(5142, 6857, readers.get(3));
    assertSplitRange(6857, 8571, readers.get(4));
    assertSplitRange(8571, 10285, readers.get(5));
    assertSplitRange(10285, 12000, readers.get(6));
  }

  
// -------------------------- STATIC METHODS --------------------------

  private static void assertSplitRange(int start, int end, InputReader<byte[]> reader) {
    CloudStorageLineInputReader r = (CloudStorageLineInputReader) reader;
    assertEquals("Start offset mismatch", start, r.startOffset);
    assertEquals("End offset mismatch", end, r.endOffset);
  }
}
