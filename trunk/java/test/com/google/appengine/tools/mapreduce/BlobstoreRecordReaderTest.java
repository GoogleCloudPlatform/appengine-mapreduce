/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for {@code BlobstoreRecordReader}.
 *
 */
public class BlobstoreRecordReaderTest extends TestCase {
  /**
   * A trailing record at the end of the file should be read, even if
   * the file isn't terminated with the terminator char.
   */
  public void test_trailingRecord() throws Exception {
    byte[] terminator = new byte[] { -1 };
    List<byte[]> content = Lists.newArrayList("I".getBytes(), "am".getBytes(),
        "RecordReader".getBytes());
    List<Integer> offsets = Lists.newArrayListWithCapacity(content.size());

    byte[] byteContent = new byte[0];
    for (byte[] entry : content) {
      offsets.add(byteContent.length);
      byteContent = Bytes.concat(byteContent, entry, terminator);
    }

    byte[] nonTerminatedRecord = "extra content".getBytes();
    content.add(nonTerminatedRecord);
    offsets.add(byteContent.length);
    // Lack of terminator is intentional
    byteContent = Bytes.concat(byteContent, nonTerminatedRecord);

    // first iteration is for the first split in a blob
    // second iteration is for non-first split
    for (final long startIndex : new long[]{0, 10}) {
     // This should read extra content too
      BlobstoreInputSplit split = new BlobstoreInputSplit("foo", startIndex,
          startIndex + byteContent.length); // the split ends one byte before record end

      checkRecordReaders(content, byteContent, startIndex, split);
    }
  }

  /**
   * Tests iterating over InputStream with persisting and restoring the state
   * of iteration
   */
  public void test_iterationsAndPerstence() throws Exception {
    byte[] terminator = new byte[] { -1 };
    List<byte[]> content = ImmutableList.of("I".getBytes(), "am".getBytes(),
        "RecordReader".getBytes());
    List<Integer> offsets = Lists.newArrayListWithCapacity(content.size());

    byte[] byteContent = new byte[0];
    int byteContentLength;
    for (byte[] entry : content) {
      offsets.add(byteContent.length);
      byteContent = Bytes.concat(byteContent, entry, terminator);
    }
    byteContentLength = byteContent.length;
    byteContent = Bytes.concat(byteContent, "extra content".getBytes(), terminator);
    // first iteration is for the first split in a blob
    // second iteration is for non-first split
    for (final long startIndex : new long[]{0, 10}) {
      BlobstoreInputSplit split = new BlobstoreInputSplit("foo", startIndex,
          startIndex + byteContentLength - 1); // the split ends one byte before record end

      checkRecordReaders(content, byteContent, startIndex, split);
    }
  }

  private void checkRecordReaders(List<byte[]> content, byte[] byteContent,
      final long startIndex, BlobstoreInputSplit split) throws IOException,
      InterruptedException {
    final byte[] finalByteContent = byteContent;
    BlobstoreRecordReader.InputStreamFactory inputStreamFactory =
        new BlobstoreRecordReader.InputStreamFactory() {
          @Override
          public InputStream getInputStream(BlobKey blobKey, long offset) throws IOException {
            ByteArrayInputStream stream = new ByteArrayInputStream(finalByteContent);
            stream.skip(offset - startIndex);
            return stream;
          }
        };
    long offset = (startIndex == 0) ? 0 : content.get(0).length + 1;
    byte[] persistedState = null;
    for (int i = (startIndex == 0) ? 0 : 1; i < content.size(); i++) {
      BlobstoreRecordReader recordReader = createRecordReader(split, inputStreamFactory,
          persistedState);
      assertTrue(recordReader.nextKeyValue());
      assertTrue(Arrays.equals(content.get(i), recordReader.getCurrentValue()));
      assertEquals(new BlobstoreRecordKey(split.getBlobKey(), split.getStartIndex() + offset),
          recordReader.getCurrentKey());
      offset += content.get(i).length + 1;
      ByteArrayOutputStream byteArrayOutput = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(byteArrayOutput);
      recordReader.write(out);
      persistedState = byteArrayOutput.toByteArray();
    }
    BlobstoreRecordReader recordReader = createRecordReader(split, inputStreamFactory,
        persistedState);
    if (recordReader.nextKeyValue()) {
      String recordContents = new String(recordReader.getCurrentValue());
      fail("Iterator returned " + recordContents + " after the last expected "
          + "read and " + recordReader.nextKeyValue()
          + " when asked if there's another one after that.");
    }
  }

  private BlobstoreRecordReader createRecordReader(BlobstoreInputSplit split,
      BlobstoreRecordReader.InputStreamFactory inputStreamFactory, byte[] persistedState)
      throws IOException, InterruptedException {
    BlobstoreRecordReader recordReader = new BlobstoreRecordReader();
    recordReader.setInputStreamFactory(inputStreamFactory);
    Configuration configuration = new Configuration();
    configuration.setInt(BlobstoreInputFormat.TERMINATOR, -1);
    recordReader.initialize(split,
        new TaskAttemptContext(configuration, new TaskAttemptID()));
    if (persistedState != null) {
      recordReader.readFields(new DataInputStream(new ByteArrayInputStream(persistedState)));
    }
    return recordReader;
  }
}
