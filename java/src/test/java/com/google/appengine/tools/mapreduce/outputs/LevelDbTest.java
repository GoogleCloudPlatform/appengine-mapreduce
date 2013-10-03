package com.google.appengine.tools.mapreduce.outputs;

import static org.junit.Assert.assertArrayEquals;

import com.google.appengine.tools.mapreduce.CorruptDataException;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.inputs.LevelDbInputReader;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Tests for {@link LevelDbInputReader} and {@link LevelDbOutputWriter}
 */
public class LevelDbTest extends TestCase {

  private static final int BLOCK_SIZE = LevelDbConstants.BLOCK_SIZE;
  private static final int MAX_SINGLE_BLOCK_RECORD_SIZE =
      LevelDbConstants.BLOCK_SIZE - LevelDbConstants.HEADER_LENGTH;

  /**
   * Writes to an in memory byte array for testing.
   *
   */
  static class ByteArrayOutputWriter extends OutputWriter<ByteBuffer> {

    private static final long serialVersionUID = -6345005368809269034L;
    ByteArrayOutputStream bout = new ByteArrayOutputStream();

    @Override
    public void write(ByteBuffer value) throws IOException {
      bout.write(SerializationUtil.getBytes(value));
    }

    @Override
    public void close() throws IOException {
      bout.close();
    }

    public byte[] toByteArray() {
      return bout.toByteArray();
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testDataCorruption() throws IOException {
    Random r = new Random(0);
    int overriddenBlockSize = 100;
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter, overriddenBlockSize);
    List<byte[]> written = writeRandomItems(r, writer, 11, 10); // Bigger than one block
    byte[] writtenData = arrayOutputWriter.toByteArray();

    for (int i = 0; i < writtenData.length; i++) {
      for (int j = 0; j < 8; j++) {
        writtenData[i] = (byte) (writtenData[i] ^ (1 << j));
        ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(writtenData);
        LevelDbInputReader reader =
            new LevelDbInputReader(Channels.newChannel(arrayInputStream), overriddenBlockSize);
        try {
          verifyWrittenData(written, reader);
          fail();
        } catch (CorruptDataException e) {
          // Expected
        }
        writtenData[i] = (byte) (writtenData[i] ^ (1 << j));
      }
    }
  }

  /**
   * This really does not test much since levelDb explicitly tolerates truncation at record
   * boundaries. So all it really validates is that we don't throw some weird error.
   */
  public void testDataTruncation() throws IOException {
    Random r = new Random(0);
    int overriddenBlockSize = 100;
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter, overriddenBlockSize);
    List<byte[]> written = writeRandomItems(r, writer, 11, 10); // Bigger than one block
    byte[] writtenData = arrayOutputWriter.toByteArray();
    for (int i = 0; i < 11 * 10; i++) {
      byte[] toRead = new byte[i];
      System.arraycopy(writtenData, 0, toRead, 0, toRead.length);
      ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(toRead);
      LevelDbInputReader reader =
          new LevelDbInputReader(Channels.newChannel(arrayInputStream), overriddenBlockSize);
      try {
        verifyWrittenData(written, reader);
        fail();
      } catch (CorruptDataException e) {
        // Expected
      } catch (NoSuchElementException e) {
        // Expected
      }
    }
  }

  public void testZeroSizeItems() throws IOException {
    verifyRandomContentRoundTrips(20000, 0);
  }

  public void testSmallItems() throws IOException {
    verifyRandomContentRoundTrips(2000, 1000);
  }

  public void testSlicingOneBlockPad() throws IOException {
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter);
    testSlicing(writer, arrayOutputWriter);
  }


  static void testSlicing(LevelDbOutputWriter writer, ByteArrayOutputWriter arrayOutputWriter)
      throws IOException {
    writer.open();
    Random r = new Random(0);
    List<byte[]> written = writeRandomItems(r, writer, 10, 100);
    writer.endSlice();
    writer.beginSlice();
    written.addAll(writeRandomItems(r, writer, 10, 100));
    byte[] writtenData = arrayOutputWriter.toByteArray();
    assertEquals(0, writtenData.length % BLOCK_SIZE);
    ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(writtenData);
    LevelDbInputReader reader = new LevelDbInputReader(Channels.newChannel(arrayInputStream));
    verifyWrittenData(written, reader);
  }

  public void testNearBlockSizeItems() throws IOException {
    for (int i = 0; i <= 64; i++) {
      verifyRandomContentRoundTrips(5, BLOCK_SIZE - i);
    }
  }

  public void testBlockAlignedItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE;
    verifyRandomContentRoundTrips(number, size);
  }

  public void testLargerThanBlockItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE + 1;
    verifyRandomContentRoundTrips(number, size);
  }

  public void testSmallerThanBlockItems() throws IOException {
    int number = 100;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE - 1;
    verifyRandomContentRoundTrips(number, size);
  }

  public void testMultiBlockAlignedItems() throws IOException {
    int number = 2;
    int size = MAX_SINGLE_BLOCK_RECORD_SIZE * 64;
    verifyRandomContentRoundTrips(number, size);
  }

  public void testLargerThanMultiBlockItems() throws IOException {
    verifyRandomContentRoundTrips(1, BLOCK_SIZE * 64 - 1);
    for (int i = 1; i < 10; i++) {
      verifyRandomContentRoundTrips(1, MAX_SINGLE_BLOCK_RECORD_SIZE * 3 - i);
      verifyRandomContentRoundTrips(1, MAX_SINGLE_BLOCK_RECORD_SIZE * 3 + i);
    }
  }

  private void verifyRandomContentRoundTrips(int number, int size) throws IOException {
    Random r = new Random(0);
    ByteArrayOutputWriter arrayOutputWriter = new ByteArrayOutputWriter();
    LevelDbOutputWriter writer = new LevelDbOutputWriter(arrayOutputWriter);
    List<byte[]> written = writeRandomItems(r, writer, number, size);
    byte[] writtenData = arrayOutputWriter.toByteArray();
    assertEquals(0, writtenData.length % BLOCK_SIZE);
    ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(writtenData);
    LevelDbInputReader reader = new LevelDbInputReader(Channels.newChannel(arrayInputStream));
    verifyWrittenData(written, reader);
  }

  static void verifyWrittenData(List<byte[]> written, LevelDbInputReader reader)
      throws IOException {
    reader.beginSlice();
    for (int i = 0; i < written.size(); i++) {
      ByteBuffer read = reader.next();
      assertArrayEquals(written.get(i), SerializationUtil.getBytes(read));
    }
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
    reader.endSlice();
  }

  static List<byte[]> writeRandomItems(Random r, LevelDbOutputWriter writer, int number, int size)
      throws IOException {
    writer.beginSlice();
    List<byte[]> written = new ArrayList<byte[]>();
    for (int i = 0; i < number; i++) {
      byte[] data = new byte[size];
      r.nextBytes(data);
      written.add(data);
      writer.write(ByteBuffer.wrap(data));
    }
    writer.endSlice();
    writer.close();
    return written;
  }

}
