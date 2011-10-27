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

import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;

import org.apache.hadoop.conf.Configuration;


/**
 * Functions for controlling and observing mapreduce jobs. Defined as interface to be more
 * test-friendly.
 *
 */
public interface AppEngineMapreduce {
  AppEngineMapreduce INSTANCE = new AppEngineMapreduceImpl();

  /**
   * Starts new MapReduce job.
   *
   * @param configuration MapReduce configuration
   * @param name MapReduce job name
   * @param basePath base path for MapReduce servlet registration. Usually it is "/mapreduce/".
   * @return the job id of the newly created MapReduce or {@code null} if the
   *         MapReduce couldn't be created.
   */
  String start(Configuration configuration, String name, String basePath);

  /**
   * Obtains mapreduce state for a job.
   * @param jobId MapReduce job id which was obtained by invoking {@link #start}.
   */
  MapReduceState getState(String jobId);
}
