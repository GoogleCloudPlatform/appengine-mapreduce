package com.google.appengine.tools.mapreduce.outputs;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.GCS_IO_BLOCK_SIZE;
import static com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.BLOCK_SIZE;
import static com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants.HEADER_LENGTH;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for {@link GoogleCloudStorageLevelDbOutputWriter}
 */
public class GoogleCloudStorageLevelDbOutputWriterTest extends TestCase {

  public void testParsesOK() throws IOException {
    runSlicingTest(10, 100);
    runSlicingTest(10, 100000);
    runSlicingTest(100, 1000);
  }

  public void testIsPadded() throws IOException {
    LevelDbTest.ByteArrayOutputWriter arrayOutputWriter = new LevelDbTest.ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(arrayOutputWriter);
    writer.beginShard();
    writer.beginSlice();
    writer.write(ByteBuffer.allocate(1));
    writer.endSlice();
    assertEquals(GCS_IO_BLOCK_SIZE, arrayOutputWriter.bout.size());
    writer.endShard();
    assertEquals(GCS_IO_BLOCK_SIZE, arrayOutputWriter.bout.size());
  }

  public void testSliceBoundries() throws IOException {
    for (int i = 0; i < 3; i++) {
      runSlicingTest(1, BLOCK_SIZE - i * HEADER_LENGTH);
      runSlicingTest(1, BLOCK_SIZE - i * HEADER_LENGTH - 1);
      runSlicingTest(1, BLOCK_SIZE - i * HEADER_LENGTH + 1);
    }
    int headersPerGcsBlock = GCS_IO_BLOCK_SIZE / BLOCK_SIZE;
    // (headersPerGcsBlock + 1) * HEADER_LENGTH is a particularly interesting case
    // When this happens there are exactly 7 bytes of 0 padding which is equal to the header length.
    for (int i = 0; i < 2 * headersPerGcsBlock; i++) {
      runSlicingTest(1, GCS_IO_BLOCK_SIZE - i * HEADER_LENGTH);
      runSlicingTest(1, GCS_IO_BLOCK_SIZE - i * HEADER_LENGTH - 1);
      runSlicingTest(1, GCS_IO_BLOCK_SIZE - i * HEADER_LENGTH + 1);
    }
  }

  private void runSlicingTest(int num, int size) throws IOException {
    LevelDbTest.ByteArrayOutputWriter arrayOutputWriter = new LevelDbTest.ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(arrayOutputWriter);
    LevelDbTest.testSlicing(writer, arrayOutputWriter, num, size);
  }
}
