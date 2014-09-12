// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 */
public class BlobstoreInputReaderTest extends BlobstoreInputTestCase {

  public static final String RECORD = "01234567890\n";
  public static final int RECORDS_COUNT = 100;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    createFile(RECORD, RECORDS_COUNT);
  }

  public void testAllSplitPoints() throws Exception {
    for (int splitPoint = 1; splitPoint < blobSize - 1; splitPoint++) {
      List<BlobstoreInputReader> readers = new ArrayList<>();
      readers.add(new BlobstoreInputReader(blobKey.getKeyString(), 0, splitPoint, (byte) '\n'));
      readers.add(
          new BlobstoreInputReader(blobKey.getKeyString(), splitPoint, blobSize, (byte) '\n'));
      verifyReaders(readers, false);
    }
  }

  public void testAllSplitPointsWithSerialization() throws Exception {
    for (int splitPoint = 1; splitPoint < blobSize - 1; splitPoint++) {
      List<BlobstoreInputReader> readers = new ArrayList<>();
      readers.add(new BlobstoreInputReader(blobKey.getKeyString(), 0, splitPoint, (byte) '\n'));
      readers.add(
          new BlobstoreInputReader(blobKey.getKeyString(), splitPoint, blobSize, (byte) '\n'));
      verifyReaders(readers, true);
    }
  }


  private void verifyReaders(List<BlobstoreInputReader> readers, boolean performSerialization)
      throws IOException {
    int recordsRead = 0;
    String recordWithoutSeparator = RECORD.substring(0, RECORD.length() - 1);

    for (BlobstoreInputReader reader : readers) {
      if (performSerialization) {
        reader = recreate(reader);
      }
      reader.beginShard();
      if (performSerialization) {
        reader = recreate(reader);
      }
      while (true) {
        reader.beginSlice();
        byte[] value;
        try {
          value = reader.next();
        } catch (NoSuchElementException e) {
          break;
        }
        assertEquals("Record mismatch", recordWithoutSeparator, new String(value));
        recordsRead++;

        reader.endSlice();
        if (performSerialization) {
          reader = recreate(reader);
        }
      }
      reader.endShard();
    }

    assertEquals("Number of records read", RECORDS_COUNT, recordsRead);
  }

  private BlobstoreInputReader recreate(BlobstoreInputReader reader) {
    byte[] bytes = SerializationUtil.serializeToByteArray(reader);
    reader = (BlobstoreInputReader) SerializationUtil.deserializeFromByteArray(bytes);
    return reader;
  }
}
