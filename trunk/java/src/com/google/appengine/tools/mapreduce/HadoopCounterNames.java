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

/**
 * These constants reimplement counter names from Hadoop that are
 * protected. The class itself is package visible because we plan
 * to move over to the the new enums introduced by
 * https://issues.apache.org/jira/browse/HADOOP-5717
 * when it gets released as part of Hadoop 0.21
 *
 */
public class HadoopCounterNames {
  private HadoopCounterNames() {

  }

  /**
   * Group name: # of input records processed.
   */
  public static final String MAP_INPUT_RECORDS_GROUP = "org.apache.hadoop.mapred.Task$Counter";
  /**
   * Counter name: # of input records processed.
   */
  public static final String MAP_INPUT_RECORDS_NAME = "MAP_INPUT_RECORDS";

  /**
   * Group name: # of input bytes processed.
   */
  // TODO(user): Hook this up.
  public static final String MAP_INPUT_BYTES_GROUP = "org.apache.hadoop.mapred.Task$Counter";
  /**
   * Counter name: # of input bytes processed.
   */
  public static final String MAP_INPUT_BYTES_NAME = "MAP_INPUT_BYTES";
}
