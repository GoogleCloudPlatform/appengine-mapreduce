package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.OutputWriter;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Random;

/**
 *
 */
public class GoogleCloudStorageFileOutputTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());

  private final GcsService gcsService = GcsServiceFactory.createGcsService();

  private static final String BUCKET = "GCSFileOutputTest";
  private static final String FILE_NAME_PATTERN = "shard-%02x";
  private static final String MIME_TYPE = "text/ascii";
  private static final int NUM_SHARDS = 10;
  private static final byte[] SMALL_CONTENT = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  // This size chosen so that it is larger than the buffer and on the first and second buffer fills
  // there will be some left over.
  private static final byte[] LARGE_CONTENT = new byte[(int) (1024 * 1024 * 2.5)];

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
    // Filling the large_content buffer with a non-repeating but consistent pattern.
    Random r = new Random(0);
    r.nextBytes(LARGE_CONTENT);
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testFilesAreWritten() throws IOException {
    GoogleCloudStorageFileOutput creator =
        new GoogleCloudStorageFileOutput(BUCKET, FILE_NAME_PATTERN, MIME_TYPE, NUM_SHARDS);
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters();
    assertEquals(NUM_SHARDS, writers.size());
    beginShard(writers);
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(SMALL_CONTENT));
      out.endSlice();
      out.close();
    }
    GoogleCloudStorageFileSet files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.getNumFiles());
    for (int i = 0; i < NUM_SHARDS; i++) {
      GcsFileMetadata metadata = gcsService.getMetadata(files.getFile(i));
      assertNotNull(metadata);
      assertEquals(SMALL_CONTENT.length, metadata.getLength());
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
    }
  }

  private void beginShard(List<? extends OutputWriter<ByteBuffer>> writers) throws IOException {
    for (OutputWriter<ByteBuffer> writer : writers) {
      writer.open();
    }
  }

  public void testSmallSlicing() throws IOException, ClassNotFoundException {
    testSlicing(SMALL_CONTENT);
  }

  public void testLargeSlicing() throws IOException, ClassNotFoundException {
    testSlicing(LARGE_CONTENT);
  }

  private void testSlicing(byte[] content) throws IOException, ClassNotFoundException {
    GoogleCloudStorageFileOutput creator =
        new GoogleCloudStorageFileOutput(BUCKET, FILE_NAME_PATTERN, MIME_TYPE, NUM_SHARDS);
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters();
    assertEquals(NUM_SHARDS, writers.size());
    beginShard(writers);
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(content));
      out.endSlice();
      out = reconstruct(out);
      out.beginSlice();
      out.write(ByteBuffer.wrap(content));
      out.endSlice();
      out.close();
    }
    GoogleCloudStorageFileSet files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.getNumFiles());
    ByteBuffer expectedContent = ByteBuffer.allocate(content.length * 2);
    expectedContent.put(content);
    expectedContent.put(content);
    for (int i = 0; i < NUM_SHARDS; i++) {
      expectedContent.rewind();
      ByteBuffer actualContent = ByteBuffer.allocate(content.length * 2 + 1);
      GcsFileMetadata metadata = gcsService.getMetadata(files.getFile(i));
      assertNotNull(metadata);
      assertEquals(expectedContent.capacity(), metadata.getLength());
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
      ReadableByteChannel readChannel = gcsService.openReadChannel(files.getFile(i), 0);
      int read = readChannel.read(actualContent);
      assertEquals(read, content.length * 2);
      actualContent.limit(actualContent.position());
      actualContent.rewind();
      assertEquals(expectedContent, actualContent);
      readChannel.close();
    }
  }

  @SuppressWarnings("unchecked")
  private OutputWriter<ByteBuffer> reconstruct(OutputWriter<ByteBuffer> writer) throws IOException,
      ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    oout.writeObject(writer);
    oout.close();
    assertTrue(bout.size() < 1000 * 1000); // Should fit in datastore.
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    return (OutputWriter<ByteBuffer>) oin.readObject();
  }

}
