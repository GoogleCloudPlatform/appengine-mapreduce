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

import com.google.common.base.Function;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;

import java.util.Arrays;
import java.util.List;

/**
 * Unit test for {@code BlobstoreInputFormat}.
 *
 * @author idk@google.com (Igor Kushnirskiy)
 */
public class BlobstoreInputFormatTest extends TestCase {

  private void assertSplits(List<InputSplit> splits, String blobKey, long blobSize,
      int shardCount) {
    assertEquals(shardCount, splits.size());
    long offset = 0;
    for (int i = 0; i < shardCount - 1; i++) {
      long splitLength = blobSize / shardCount;
      assertTrue(splits.contains(new BlobstoreInputSplit(blobKey, offset, offset + splitLength)));
      offset += splitLength;
    }
    assertTrue(splits.contains(new BlobstoreInputSplit(blobKey, offset, blobSize)));
  }

  /** Tests that splits are created of right sizes. */
  public void test_getSplitsInternal() throws Exception {
    BlobstoreInputFormat inputFormat = new BlobstoreInputFormat();
    String blobKey = "blobKey";

    long blobSize = 1024;
    for (int shardCount : Arrays.asList(1, 3)) {
      assertSplits(inputFormat.getSplits(blobKey, blobSize, shardCount), blobKey, blobSize,
          shardCount);
    }
  }

  /**  Tests that splits are created of right sizes. */
  public void test_createRecordReader() throws Exception {
    BlobstoreInputFormat inputFormat = new BlobstoreInputFormat();
    InputSplit inputSplit = EasyMock.createMock(InputSplit.class);
    TaskAttemptContext taskAttemptContext = EasyMock.createMock(TaskAttemptContext.class);

    RecordReader<BlobstoreRecordKey, byte[]> recordReader = inputFormat
        .createRecordReader(inputSplit, taskAttemptContext);
    assertTrue(BlobstoreRecordReader.class.isInstance(recordReader));
  }

  /**
   * Tests that public {@code getSplits} passes expected arguments to package
   * private one from the {@code JobContext}.
   */
  public void test_getSplits() throws Exception {
    String blobKey = "blobKey";
    int shardCount = 3;
    long blobSize = 1024;
    IMocksControl control = EasyMock.createControl();

    @SuppressWarnings("unchecked")
    Function<String, Long>  blobKeyToSize = control.createMock(Function.class);

    JobContext jobContext = control.createMock(JobContext.class);
    Configuration configuration = control.createMock(Configuration.class);
    EasyMock.expect(jobContext.getConfiguration()).andReturn(configuration).anyTimes();
    EasyMock.expect(configuration.get(BlobstoreInputFormat.BLOB_KEYS)).andReturn(blobKey);
    EasyMock.expect(configuration.getInt(BlobstoreInputFormat.SHARD_COUNT,
        BlobstoreInputFormat.DEFAULT_SHARD_COUNT)).andReturn(shardCount);
    EasyMock.expect(blobKeyToSize.apply(blobKey)).andReturn(blobSize);
    // this is what we are testing

    control.replay();
    BlobstoreInputFormat inputFormat = new BlobstoreInputFormat();

    inputFormat.setBlobKeyToSize(blobKeyToSize);
    assertSplits(inputFormat.getSplits(jobContext), blobKey, blobSize, shardCount);

    control.verify();
  }
}
