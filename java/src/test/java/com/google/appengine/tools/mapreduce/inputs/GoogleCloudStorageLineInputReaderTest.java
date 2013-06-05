package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class GoogleCloudStorageLineInputReaderTest extends GoogleCloudStorageLineInputTestCase {

  private static final String FILENAME = "GoogleCloudStorageLineInputReaderTestFile";
  private static final String BUCKET = "GoogleCloudStorageInputReaderTestBucket";
  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 10;

  GcsFilename filename = new GcsFilename(BUCKET, FILENAME);
  long fileSize;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fileSize = createFile(filename, RECORD, RECORDS_COUNT);
  }
  public void testSingleSplitPoint() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers =
        new ArrayList<GoogleCloudStorageLineInputReader>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n'));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n'));
    verifyReaders(readers, false);
  }

  public void testSingleSplitPointsWithSerialization() throws Exception {
    List<GoogleCloudStorageLineInputReader> readers =
        new ArrayList<GoogleCloudStorageLineInputReader>();
    readers.add(new GoogleCloudStorageLineInputReader(filename, 0, RECORD.length(), (byte) '\n'));
    readers.add(
        new GoogleCloudStorageLineInputReader(filename, RECORD.length(), fileSize, (byte) '\n'));
    verifyReaders(readers, true);
  }

  public void testAllSplitPoints() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers =
          new ArrayList<GoogleCloudStorageLineInputReader>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n'));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n'));
      verifyReaders(readers, false);
    }
  }

  public void testAllSplitPointsWithSerialization() throws Exception {
    for (int splitPoint = 1; splitPoint < fileSize - 1; splitPoint++) {
      List<GoogleCloudStorageLineInputReader> readers =
          new ArrayList<GoogleCloudStorageLineInputReader>();
      readers.add(new GoogleCloudStorageLineInputReader(filename, 0, splitPoint, (byte) '\n'));
      readers.add(
          new GoogleCloudStorageLineInputReader(filename, splitPoint, fileSize, (byte) '\n'));
      verifyReaders(readers, true);
    }
  }


  private void verifyReaders(
      List<GoogleCloudStorageLineInputReader> readers, boolean performSerialization)
      throws IOException {
    int recordsRead = 0;
    long lastOffset = -1;
    String recordWithoutSeparator = RECORD.substring(0, RECORD.length() - 1);

    for (GoogleCloudStorageLineInputReader reader : readers) {
      reader.beginSlice();
      while (true) {
        byte[] value;
        try {
          value = reader.next();
        } catch (NoSuchElementException e) {
          break;
        }
        assertEquals("Record mismatch", recordWithoutSeparator, new String(value));
        recordsRead++;

        if (performSerialization) {
          reader.endSlice();
          byte[] bytes = SerializationUtil.serializeToByteArray(reader);
          reader =
              (GoogleCloudStorageLineInputReader) SerializationUtil.deserializeFromByteArray(bytes);
          reader.beginSlice();
        }
      }
    }

    assertEquals("Number of records read", RECORDS_COUNT, recordsRead);
  }
}
