package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.mapreduce.impl.util.LevelDbConstants;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageLevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
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
      new LocalTaskQueueTestConfig(), new LocalFileServiceTestConfig(),
      new LocalBlobstoreServiceTestConfig(), new LocalDatastoreServiceTestConfig());

  
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
    writer.open();
    writer.beginSlice();
    while (gen.hasNext()) {
      writer.write(gen.next());
    }
    writer.endSlice();
    writer.close();
  }
  
  public void testReading() throws IOException {
    writeData(filename, new ByteBufferGenerator(100));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, BLOCK_SIZE * 2);
    ByteBufferGenerator expected = new ByteBufferGenerator(100);
    reader.beginSlice();
    while (expected.hasNext()) {
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
    }
    verifyEmpty(reader);
    reader.endSlice();
  }
  
  public void testReadingWithSerialization() throws IOException, ClassNotFoundException {
    writeData(filename, new ByteBufferGenerator(100));
    GoogleCloudStorageLevelDbInputReader reader =
        new GoogleCloudStorageLevelDbInputReader(filename, BLOCK_SIZE * 2);
    ByteBufferGenerator expected = new ByteBufferGenerator(100);
    while (expected.hasNext()) {
      reader = reconstruct(reader);
      reader.beginSlice();
      ByteBuffer read = reader.next();
      assertEquals(expected.next(), read);
      reader.endSlice();
    }
    reader = reconstruct(reader);
    reader.beginSlice();
    verifyEmpty(reader);
    reader.endSlice();
  }
  
  private GoogleCloudStorageLevelDbInputReader reconstruct(
      GoogleCloudStorageLevelDbInputReader reader) throws IOException, ClassNotFoundException { 
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    try {
      oout.writeObject(reader);
    } finally {
      oout.close();
    }
    ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()));
    return (GoogleCloudStorageLevelDbInputReader) in.readObject();
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
