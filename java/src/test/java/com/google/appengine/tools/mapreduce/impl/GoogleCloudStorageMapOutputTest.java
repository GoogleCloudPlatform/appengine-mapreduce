// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Test class for {@link GoogleCloudStorageMapOutput}.
 *
 */
public class GoogleCloudStorageMapOutputTest extends TestCase {

  private static final String BUCKET = "GoogleCloudStorageMapOutputTest";
  private static final String JOB = "JOB1";
  private static final Marshaller<Long> KEY_MARSHALLER = Marshallers.getLongMarshaller();
  private static final Marshaller<String> VALUE_MARSHALLER = Marshallers.getStringMarshaller();
  private static final int MAX_FILES_PER_COMPOSE_OPERATION = 32;
  private static final int MAX_FILES_PER_COMPOSED_FILE = MAX_FILES_PER_COMPOSE_OPERATION * 2;
  private static final Random RND = new SecureRandom();

  static {
    // TODO(user): consider using a GCS client property that would enforce that in DEV
    System.setProperty("com.google.appengine.tools.mapreduce.impl"
            + ".GoogleCloudStorageMapOutputWriter.MAX_FILES_PER_COMPOSE",
            String.valueOf(MAX_FILES_PER_COMPOSED_FILE));
  }

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalFileServiceTestConfig());

  @Override
  public void setUp() {
    helper.setUp();
  }

  @Override
  public void tearDown() {
    helper.tearDown();
  }

  public void testNoContent() throws IOException {
    writeAndVerifyContent(0, 0, 0);
  }

  public void testSingleFilePart() throws IOException {
    writeAndVerifyContent(1, 100, 100);
    writeAndVerifyContent(MAX_FILES_PER_COMPOSE_OPERATION + 1, 10, 10);
    writeAndVerifyContent(MAX_FILES_PER_COMPOSED_FILE, 1, 1);
  }

  public void testWithMultipleFileParts() throws IOException  {
    writeAndVerifyContent(MAX_FILES_PER_COMPOSED_FILE + 1, 1, 1);
  }

  private List<KeyValue<Long, String>> createRandomValues(int maxValues, int maxValueSize) {
    if (maxValues == 0) {
      return Collections.emptyList();
    }
    int count = 1 + RND.nextInt(maxValues);
    List<KeyValue<Long, String>> values = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      int strlen = RND.nextInt(maxValueSize);
      StringBuilder value = new StringBuilder(strlen);
      for (int j = 0; j < strlen; j++) {
        value.append(RND.nextInt(10));
      }
      values.add(new KeyValue<>(RND.nextLong(), value.toString()));
    }
    return values;
  }

  private void writeAndVerifyContent(int slices, int maxValuesPerSlice, int maxValueSize)
      throws IOException  {
    GoogleCloudStorageMapOutput<Long, String> output = new GoogleCloudStorageMapOutput<>(BUCKET,
        JOB, KEY_MARSHALLER, VALUE_MARSHALLER, new Sharder() {
          private static final long serialVersionUID = 1L;

          @Override
          public int getNumShards() {
            return 1;
          }

          @Override
          public int getShardForKey(ByteBuffer key) {
            return 0;
          }
        });
    List<? extends OutputWriter<KeyValue<Long, String>>> writers = output.createWriters(1);
    assertEquals(1, writers.size());
    List<KeyValue<Long, String>> values = new ArrayList<>(slices);
    OutputWriter<KeyValue<Long, String>> writer = writers.get(0);
    writer.beginShard();
    for (int i = 0; i < slices; i++) {
      writer.beginSlice();
      for (KeyValue<Long, String> value : createRandomValues(maxValuesPerSlice, maxValueSize)) {
        writer.write(value);
        values.add(value);
      }
      writer.endSlice();
    }
    writer.endShard();

    FilesByShard filesByShard = output.finish(writers);
    assertEquals(1, filesByShard.getShardCount());
    List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> input =
        new GoogleCloudStorageSortInput(filesByShard).createReaders();
    assertEquals(1, input.size());
    int expectedFiles = (int) Math.ceil((double) slices / MAX_FILES_PER_COMPOSED_FILE);
    assertEquals(expectedFiles, filesByShard.getFilesForShard(0).getNumFiles());
    Iterator<KeyValue<Long, String>> shardValues = values.iterator();
    InputReader<KeyValue<ByteBuffer, ByteBuffer>> reader = input.get(0);
    reader.beginShard();
    reader.beginSlice();
    try {
      while (true) {
        KeyValue<ByteBuffer, ByteBuffer> kv = reader.next();
        KeyValue<Long, String> expected = shardValues.next();
        KeyValue<Long, String> value = new KeyValue<>(KEY_MARSHALLER.fromBytes(kv.getKey()),
            VALUE_MARSHALLER.fromBytes(kv.getValue()));
        assertEquals(expected, value);
      }
    } catch (NoSuchElementException expected) {
      // reader has no more data.
    }
    reader.endSlice();
    reader.endShard();
    assertFalse(shardValues.hasNext());
  }
}
