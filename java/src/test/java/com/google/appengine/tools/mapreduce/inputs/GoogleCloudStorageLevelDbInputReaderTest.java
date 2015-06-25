package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Tests for {@link GoogleCloudStorageLevelDbInput}
 */
public class GoogleCloudStorageLevelDbInputReaderTest extends TestCase {

  private static final int BLOCK_SIZE = LevelDbConstants.BLOCK_SIZE;
  GcsFilename filename = new GcsFilename("Bucket", "GoogleCloudStorageLevelDbInputReaderTest");

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(),
      new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());


  @Override
  @Before
  public void setUp() throws Exception {
    helper.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    GcsServiceFactory.createGcsService().delete(filename);
    helper.tearDown();
  }

  private class ByteBufferGenerator implements Iterator<ByteBuffer> {
    Random r;
    int remaining;

    ByteBufferGenerator(int count) {
      this.r = new Random(count);
      this.remaining = count;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0;
    }

    @Override
    public ByteBuffer next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      remaining--;
      int size = r.nextInt(r.nextInt(r.nextInt(BLOCK_SIZE * 4))); // Skew small occasionally large
      byte[] bytes = new byte[size];
      r.nextBytes(bytes);
      return ByteBuffer.wrap(bytes);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  public void writeData(GcsFilename filename, ByteBufferGenerator gen) throws IOException {
    LevelDbOutputWriter writer = new GoogleCloudStorageLevelDbOutputWriter(
        new GoogleCloudStorageFileOutputWriter(filename, "TestData"));
    writer.beginShard();
    writer.beginSlice();
    while (gen.hasNext()) {
      writer.write(gen.next());
    }
    writer.endSlice();
    writer.endShard();
  }

  public void testReading() throws IOException {
    writeData(filename, new ByteBufferGenerator(100));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, BLOCK_SIZE * 2);
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(100);
    reader.beginSlice();
    while (expected.hasNext()) {
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
    }
    verifyEmpty(reader);
    reader.endSlice();
  }

  public void testRecordsDontChange() throws IOException {
    writeData(filename, new ByteBufferGenerator(1000));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, BLOCK_SIZE * 2);
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(1000);
    reader.beginSlice();
    ArrayList<ByteBuffer> recordsRead = new ArrayList<>();
    try {
      while (true) {
        recordsRead.add(reader.next());
      }
    } catch (NoSuchElementException e) {
      // used a break
    }
    for (int i = 0; i < recordsRead.size(); i++) {
      assertTrue(expected.hasNext());
      ByteBuffer read = recordsRead.get(i);
      assertEquals(expected.next(), read);
    }
    verifyEmpty(reader);
    reader.endSlice();
  }

  public void testReadingWithSerialization() throws IOException, ClassNotFoundException {
    writeData(filename, new ByteBufferGenerator(100));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, BLOCK_SIZE * 2);
    reader.beginShard();
    ByteBufferGenerator expected = new ByteBufferGenerator(100);
    while (expected.hasNext()) {
      reader = SerializationUtil.clone(reader);
      reader.beginSlice();
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
      reader.endSlice();
    }
    reader = SerializationUtil.clone(reader);
    reader.beginSlice();
    verifyEmpty(reader);
    reader.endSlice();
  }

  private void verifyEmpty(GoogleCloudStorageLevelDbInputReader reader) throws IOException {
    try {
      reader.next();
      fail();
    } catch (NoSuchElementException e) {
      // expected
    }
  }
}
