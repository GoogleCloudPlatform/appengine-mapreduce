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

import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * An {@code InputFormat} for reading from the AppEngine Blobstore.
 * <p>
 * Note: The terminator byte is not part of the value {@code byte[]} sent to the mapper.
 * <p> Expects the following values to be present in the mapreduce configuration:
 * <ul>
 *   <li>{@code BlobstoreInputFormat.BLOB_KEYS} with the string value of the blob key to map over
 *   </li>
 *   <li>{@code BlobstoreInputFormat.SHARD_COUNT} with positive integer value of the number of
 *       shards to use.
 *   </li>
 * </ul>
 * <p> The following configuration values are optional:
 * <ul>
 *   <li> {@code BlobstoreInputFormat.TERMINATOR} with integer number in the
 *    [{@code Byte.MIN_VALUE}, {@code Byte.MAX_VALUE}] range. The default value is {@code '\n'}.
 *   </li>
 * </ul>
 */
// TODO(user): add support for splitting on an arbitrary byte sequence.
public final class BlobstoreInputFormat extends InputFormat<BlobstoreRecordKey, byte[]> {

  // TODO(user): add support for mapping over multiple blobs.
  public static final String BLOB_KEYS =
      "mapreduce.mapper.inputformat.blobstoreinputformat.blobkeys";

  public static final String TERMINATOR =
      "mapreduce.mapper.inputformat.blobstoreinputformat.terminator";

  public static final String SHARD_COUNT =  "mapreduce.mapper.shardcount";

  @VisibleForTesting
  static final int DEFAULT_SHARD_COUNT = 10;

  private Function<String, Long> blobKeyToSize = new Function<String, Long>() {
    @Override
    public Long apply(String key) {
      return new BlobInfoFactory().loadBlobInfo(new BlobKey(key)).getSize();
    }
  };

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    Configuration configuration = context.getConfiguration();
    String blobKey = configuration.get(BLOB_KEYS);
    int shardCount = configuration.getInt(SHARD_COUNT, DEFAULT_SHARD_COUNT);
    long blobSize = blobKeyToSize.apply(blobKey);
    return getSplits(blobKey, blobSize, shardCount);
  }

  @Override
  public RecordReader<BlobstoreRecordKey, byte[]> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new BlobstoreRecordReader();
  }

  @VisibleForTesting
  void setBlobKeyToSize(Function<String, Long> blobKeyToSize) {
    this.blobKeyToSize = Preconditions.checkNotNull(blobKeyToSize);
  }

  @VisibleForTesting
  List<InputSplit> getSplits(String blobKey, long blobSize, int shardCount) {
    Preconditions.checkNotNull(blobKey);
    Preconditions.checkArgument(shardCount > 0);
    Preconditions.checkArgument(blobSize >= 0);

    long splitLength = blobSize / shardCount;
    /*
     * Currently a single shard gets assigned a single split.
     * shardCount is only a hint for a number of splits we create.
     * If a shard workload is to small we want to reduce a number of shards.
     */
    if (splitLength == 0) {
      splitLength = 1;
      shardCount = (int) blobSize;
    }

    ImmutableList.Builder<InputSplit> splitsBuilder = ImmutableList.builder();

    long startOffset = 0;
    for (int i = 1; i < shardCount; i++) {
      long endOffset = i * splitLength;
      splitsBuilder.add(new BlobstoreInputSplit(blobKey, startOffset, endOffset));
      startOffset = endOffset;
    }
    splitsBuilder.add(new BlobstoreInputSplit(blobKey, startOffset, blobSize));
    return splitsBuilder.build();
  }
}
