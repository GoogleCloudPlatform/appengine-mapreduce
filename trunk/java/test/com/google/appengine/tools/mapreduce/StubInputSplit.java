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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Stub input split. Contains the info for generating a predetermined series
 * of records.
 * 
 */
public class StubInputSplit extends InputSplit implements Writable {
  private int id;
  static final List<IntWritable> KEYS = Arrays.asList(
      new IntWritable(10), new IntWritable(9), new IntWritable(8));
  
  // Only intended for deserialization
  public StubInputSplit() {
    
  }
  
  public StubInputSplit(int id) {
    this.id = id;
  }
   
  public IntWritable getKey(int offset) {
    return KEYS.get(offset);
  }
  
  public List<IntWritable> getKeys() {
    return KEYS;
  }
  
  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return null;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
  }  
}