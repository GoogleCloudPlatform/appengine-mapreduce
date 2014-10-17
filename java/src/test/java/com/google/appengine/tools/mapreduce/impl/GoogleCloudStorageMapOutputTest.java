// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.ThreadManager;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Sharder;

import junit.framework.TestCase;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test class for {@link GoogleCloudStorageMapOutput}.
 *
 */
public class GoogleCloudStorageMapOutputTest extends TestCase {

  private static final String BUCKET = "GoogleCloudStorageMapOutputTest";
  private static final String JOB = "JOB1";
  private static final Marshaller<Long> KEY_MARSHALLER = Marshallers.getLongMarshaller();
  private static final Marshaller<String> VALUE_MARSHALLER = Marshallers.getStringMarshaller();
  private static final int NUM_SHARDS = 2;
  private static final int MAX_FILES_PER_COMPOSE_OPERATION = 32;
  private static final int MAX_FILES_PER_COMPOSED_FILE = MAX_FILES_PER_COMPOSE_OPERATION * 2;
  private static final Random RND = new SecureRandom();

  static {
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

  public void testNoContent() throws Exception {
    writeAndVerifyContent(0, 0, 0);
  }

  public void testNoCompositePart() throws Exception {
    writeAndVerifyContent(1, 100, 100);
    writeAndVerifyContent(MAX_FILES_PER_COMPOSE_OPERATION, 10, 10);
  }

  public void testWithCompositeParts() throws Exception {
    writeAndVerifyContent(MAX_FILES_PER_COMPOSE_OPERATION + 1, 10, 10);
    writeAndVerifyContent(MAX_FILES_PER_COMPOSED_FILE, 1, 1);
  }

  public void xtestWithMultipleParts() throws Exception {
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

  private void writeAndVerifyContent(final int slices, final int maxValuesPerSlice,
      final int maxValueSize) throws Exception {
    final AtomicInteger shardId = new AtomicInteger();
    GoogleCloudStorageMapOutput<Long, String> output = new GoogleCloudStorageMapOutput<>(BUCKET,
        JOB, KEY_MARSHALLER, VALUE_MARSHALLER, new Sharder() {
          private static final long serialVersionUID = 1L;

          @Override
          public int getNumShards() {
            return NUM_SHARDS;
          }

          @Override
          public int getShardForKey(ByteBuffer key) {
            return shardId.intValue();
          }
        });
    final List<? extends OutputWriter<KeyValue<Long, String>>> writers =
        output.createWriters(NUM_SHARDS);
    final List<List<KeyValue<Long, String>>> values = new ArrayList<>(NUM_SHARDS);
    final CountDownLatch latch1 = new CountDownLatch(NUM_SHARDS);
    final AtomicReference<Exception> exception = new AtomicReference<>();
    for (final OutputWriter<KeyValue<Long, String>> writer : writers) {
      ThreadManager.createBackgroundThread(new Runnable() {
        @Override
        public void run() {
          List<KeyValue<Long, String>> valuesPerShard = new ArrayList<>();
          values.add(valuesPerShard);
          try {
            writer.beginShard();
            for (int i = 0; i < slices; i++) {
              writer.beginSlice();
              for (KeyValue<Long, String> value :
                  createRandomValues(maxValuesPerSlice, maxValueSize)) {
                writer.write(value);
                valuesPerShard.add(value);
              }
              writer.endSlice();
            }
            writer.endShard();
          } catch (Exception ex) {
            exception.set(ex);
          } finally {
            shardId.incrementAndGet();
            latch1.countDown();
          }
        }
      }).start();
    }

    latch1.await();
    if (exception.get() != null) {
      throw exception.get();
    }

    final CountDownLatch latch2 = new CountDownLatch(NUM_SHARDS);
    final FilesByShard filesByShard = output.finish(writers);
    final List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> input =
        new GoogleCloudStorageSortInput(filesByShard).createReaders();
    for (int i = 0; i < NUM_SHARDS; i++) {
      final int shardIdx = i;
      ThreadManager.createBackgroundThread(new Runnable() {
        @Override
        public void run() {
          try {
            int expectedFiles = (int) Math.ceil((double) slices / MAX_FILES_PER_COMPOSED_FILE);
            assertEquals(expectedFiles, filesByShard.getFilesForShard(shardIdx).getNumFiles());
            Iterator<KeyValue<Long, String>> shardValues = values.get(shardIdx).iterator();
            InputReader<KeyValue<ByteBuffer, ByteBuffer>> reader = input.get(shardIdx);
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
          } catch (Exception ex) {
            exception.set(ex);
          } finally {
            latch2.countDown();
          }
        }
      }).start();
    }
    latch2.await();
    if (exception.get() != null) {
      throw exception.get();
    }
  }
}
