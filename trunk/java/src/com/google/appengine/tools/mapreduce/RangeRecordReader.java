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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * The record reader class for {@link RangeInputFormat}.
 *
 */
public class RangeRecordReader extends RecordReader<Long, NullWritable>
    implements Writable {

  private RangeInputSplit split;
  private long currentKey;

  public RangeRecordReader() {
    currentKey = -1;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Long getCurrentKey() throws IOException, InterruptedException {
    if (split == null) {
      throw new RuntimeException(
          "Attempted to read from RangeRecordReader without a RangeInputSplit");
    }
    if (currentKey < 0 || currentKey >= split.getSplitEnd()) {
      return null;
    }
    return currentKey;
  }

  @Override
  public NullWritable getCurrentValue() throws IOException,
      InterruptedException {
    return null;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (currentKey < 0) {
      return 0;
    }

    return ((float) currentKey - split.getSplitStart())
        / (split.getSplitEnd() - split.getSplitStart());
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException {
    this.split = (RangeInputSplit) split;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentKey < 0) {
      currentKey = split.getSplitStart();
    } else {
      currentKey++;
    }

    if (currentKey >= split.getSplitEnd()) {
      return false;
    }

    return true;
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    currentKey = input.readLong();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(currentKey);
  }

}
