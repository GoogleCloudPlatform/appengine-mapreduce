package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
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

/**
 *
 */
public class CloudStorageFileOutputTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());

  private final GcsService gcsService = GcsServiceFactory.createGcsService();

  private static final String BUCKET = "CloudFileOutputTest";
  private static final String FILE_NAME_PATTERN = "shard-%02x";
  private static final String MIME_TYPE = "text/ascii";
  private static final int NUM_SHARDS = 10;
  private static final byte[] CONTENT = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};


  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  public void testFilesAreWritten() throws IOException {
    CloudStorageFileOutput creator =
        new CloudStorageFileOutput(BUCKET, FILE_NAME_PATTERN, MIME_TYPE, NUM_SHARDS);
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters();
    assertEquals(NUM_SHARDS, writers.size());
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(CONTENT));
      out.endSlice();
      out.close();
    }
    List<GcsFilename> files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.size());
    for (int i = 0; i < NUM_SHARDS; i++) {
      GcsFileMetadata metadata = gcsService.getMetadata(files.get(i));
      assertNotNull(metadata);
      assertEquals(CONTENT.length, metadata.getLength());
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
    }
  }

  public void testSlicing() throws IOException, ClassNotFoundException {
    CloudStorageFileOutput creator =
        new CloudStorageFileOutput(BUCKET, FILE_NAME_PATTERN, MIME_TYPE, NUM_SHARDS);
    List<? extends OutputWriter<ByteBuffer>> writers = creator.createWriters();
    assertEquals(NUM_SHARDS, writers.size());
    for (int i = 0; i < NUM_SHARDS; i++) {
      OutputWriter<ByteBuffer> out = writers.get(i);
      out.beginSlice();
      out.write(ByteBuffer.wrap(CONTENT));
      out.endSlice();
      out = reconstruct(out);
      out.beginSlice();
      out.write(ByteBuffer.wrap(CONTENT));
      out.endSlice();
      out.close();
    }
    List<GcsFilename> files = creator.finish(writers);
    assertEquals(NUM_SHARDS, files.size());
    ByteBuffer expectedContent = ByteBuffer.allocate(CONTENT.length * 2);
    expectedContent.put(CONTENT);
    expectedContent.put(CONTENT);
    for (int i = 0; i < NUM_SHARDS; i++) {
      expectedContent.rewind();
      ByteBuffer actualContent = ByteBuffer.allocate(CONTENT.length * 2 + 1);
      GcsFileMetadata metadata = gcsService.getMetadata(files.get(i));
      assertNotNull(metadata);
      assertEquals(expectedContent.capacity(), metadata.getLength());
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
      ReadableByteChannel readChannel = gcsService.openReadChannel(files.get(i), 0);
      int read = readChannel.read(actualContent);
      assertEquals(read, CONTENT.length * 2);
      actualContent.limit(actualContent.position());
      actualContent.rewind();
      assertEquals(expectedContent, actualContent);
    }
  }

  @SuppressWarnings("unchecked")
  private OutputWriter<ByteBuffer> reconstruct(OutputWriter<ByteBuffer> writer)
      throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream oout = new ObjectOutputStream(bout);
    oout.writeObject(writer);
    oout.close();
    ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
    ObjectInputStream oin = new ObjectInputStream(bin);
    return (OutputWriter<ByteBuffer>) oin.readObject();
  }

}
