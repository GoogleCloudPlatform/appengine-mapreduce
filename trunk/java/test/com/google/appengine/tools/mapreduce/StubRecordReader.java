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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Reader that just passes through the values from a {@link StubInputSplit}.
 *
 *
 */
public class StubRecordReader extends RecordReader<IntWritable, IntWritable> implements
    Writable {
  private StubInputSplit split;
  private int currentOffset;

  public InputSplit getSplit() {
    return split;
  }

  @Override
  public void close() {
  }

  @Override
  public IntWritable getCurrentKey() {
    return split.getKey(currentOffset);
  }

  @Override
  public IntWritable getCurrentValue() {
    // We have no values.
    return null;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) {
    this.split = (StubInputSplit) split;
    currentOffset = -1;
  }

  @Override
  public boolean nextKeyValue() {
    return ++currentOffset < split.getKeys().size();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
  }

  @Override
  public void write(DataOutput out) throws IOException {
  }
}
