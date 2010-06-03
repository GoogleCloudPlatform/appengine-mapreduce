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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Arrays;
import java.util.List;

/**
 * A format that creates a predetermined set of StubInputSplits.
 * 
 * @author frew@google.com (Fred Wulff)
 */
public class StubInputFormat extends InputFormat<IntWritable, IntWritable> {
  @Override
  public StubRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new StubRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    return Arrays.asList((InputSplit) new StubInputSplit(1), new StubInputSplit(2));
  }  
}
