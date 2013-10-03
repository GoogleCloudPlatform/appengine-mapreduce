package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Tests for {@link GoogleCloudStorageLevelDbOutputWriter}
 */
public class GoogleCloudStorageLevelDbOutputWriterTest extends TestCase {

  public void testParsesOK() throws IOException {
    LevelDbTest.ByteArrayOutputWriter arrayOutputWriter = new LevelDbTest.ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(arrayOutputWriter);
    LevelDbTest.testSlicing(writer, arrayOutputWriter);
  }
  
  public void testIsPadded() throws IOException {
    LevelDbTest.ByteArrayOutputWriter arrayOutputWriter = new LevelDbTest.ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(arrayOutputWriter);
    writer.beginSlice();
    writer.write(ByteBuffer.allocate(1));
    writer.endSlice();
    assertEquals(MapReduceConstants.GCS_IO_BLOCK_SIZE, arrayOutputWriter.bout.size());
    writer.close();
    assertEquals(MapReduceConstants.GCS_IO_BLOCK_SIZE, arrayOutputWriter.bout.size());
  }

}
