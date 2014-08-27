package com.google.appengine.tools.mapreduce.outputs;

import com.google.appengine.tools.cloudstorage.GcsFileMetadata;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.BigQueryConstants;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SizeSegmentedGoogleCloudStorageFileOutputTest extends TestCase {

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());
  private final GcsService gcsService = GcsServiceFactory.createGcsService();

  private static final String BUCKET = "test-bigquery-loader";
  private static final String MIME_TYPE = "application/json";

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    helper.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    helper.tearDown();
  }

  public void testFilesWritten() throws IOException, ClassNotFoundException {
    int segmentSizeLimit = 10;
    String fileNamePattern = String.format(BigQueryConstants.GCS_FILE_NAME_FORMAT, "testJob");
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(BUCKET, segmentSizeLimit, fileNamePattern,
            BigQueryConstants.MIME_TYPE);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    List<OutputWriter<ByteBuffer>> finished = new ArrayList<>();
    assertEquals(5, writers.size());
    for (OutputWriter<ByteBuffer> w : writers) {
      w.beginShard();
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = reconstruct(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w = reconstruct(w);
      w.beginSlice();
      w.write(ByteBuffer.wrap(new byte[9]));
      w.endSlice();
      w.endShard();
      finished.add(w);
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(finished);
    assertEquals(15, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      GcsFileMetadata metadata = gcsService.getMetadata(filesWritten.getFile(i));
      assertNotNull(metadata);
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
    }
  }

  public void testSegmentation() throws IOException {
    int segmentSizeLimit = 10;
    SizeSegmentedGoogleCloudStorageFileOutput segmenter =
        new SizeSegmentedGoogleCloudStorageFileOutput(BUCKET, segmentSizeLimit, "testJob",
            BigQueryConstants.MIME_TYPE);
    List<? extends OutputWriter<ByteBuffer>> writers = segmenter.createWriters(5);
    int countFiles = 0;
    for (OutputWriter<ByteBuffer> w : writers) {
      writeMultipleValues(w, 3, 9);
      countFiles += 3;
    }
    GoogleCloudStorageFileSet filesWritten = segmenter.finish(writers);
    assertEquals(countFiles, filesWritten.getNumFiles());
    for (int i = 0; i < filesWritten.getNumFiles(); i++) {
      GcsFileMetadata metadata = gcsService.getMetadata(filesWritten.getFile(i));
      assertNotNull(metadata);
      assertEquals(MIME_TYPE, metadata.getOptions().getMimeType());
    }
  }

  /**
   * @param writer
   * @throws IOException
   */
  private void writeMultipleValues(OutputWriter<ByteBuffer> writer, int count, int size)
      throws IOException {
    writer.beginShard();
    writer.beginSlice();
    Random r = new Random(0);
    for (int i = 0; i < count; i++) {
      byte[] data = new byte[size];
      r.nextBytes(data);
      writer.write(ByteBuffer.wrap(data));
    }
    writer.endSlice();
    writer.endShard();
  }

  @SuppressWarnings("unchecked")
  private static <T> T reconstruct(T obj) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    try (ObjectOutputStream oout = new ObjectOutputStream(bout)) {
      oout.writeObject(obj);
    }
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bout.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
