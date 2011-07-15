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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * An input format that produces integer keys in a given range.
 *
 * The range is specified to be from the value of
 * {@code RangeInputFormat.RANGE_START_KEY} inclusive to the value of
 * {@code RangeInputFormat.RANGE_END_KEY} exclusive.
 *
 * The number of shards to use is specified by the value of
 * {@code RangeInputFormat.SHARD_COUNT_KEY}
 *
 *
 */
public class RangeInputFormat extends InputFormat<Long, NullWritable> {
  /**
   * Key for storing the shard count in a {@code Configuration}
   */
  public static final String SHARD_COUNT_KEY = "mapreduce.mapper.shardcount";

  /**
   * Default number of input shards.
   */
  public static final int DEFAULT_SHARD_COUNT = 4;

  /**
   * Key for inclusive start value for the range to process.
   */
  public static final String RANGE_START_KEY
      = "mapreduce.mapper.inputformat.rangeinputformat.range_start";

  /**
   * Key for exclusive end value for the range to process.
   */
  public static final String RANGE_END_KEY
      = "mapreduce.mapper.inputformat.rangeinputformat.range_end";

  @Override
  public RecordReader<Long, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    return new RangeRecordReader();
  }

  private long getNonNegativeLong(Configuration conf, String key) throws IOException {
    long retVal = conf.getLong(key, -1L);
    if (retVal < 0) {
      throw new InvalidConfigurationException("Invalid or nonexistent value for " + key);
    }
    return retVal;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    Configuration conf = context.getConfiguration();
    long shardCount = conf.getInt(SHARD_COUNT_KEY, DEFAULT_SHARD_COUNT);
    long rangeStart = getNonNegativeLong(conf, RANGE_START_KEY);
    long rangeEnd = getNonNegativeLong(conf, RANGE_END_KEY);
    if (rangeStart >= rangeEnd) {
      throw new InvalidConfigurationException(
          "Invalid range. Start: " + rangeStart + " >= end: " + rangeEnd);
    }

    double increment = ((double) rangeEnd - rangeStart) / shardCount;
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
    for (int i = 0; i < shardCount - 1; i++) {
      splits.add(new RangeInputSplit(rangeStart + Math.round(i * increment),
          rangeStart + Math.round((i + 1) * increment)));
    }

    // Make sure that the final split hits end
    splits.add(new RangeInputSplit(
        rangeStart + Math.round((shardCount - 1) * increment),
        rangeEnd));

    return splits;
  }
}
