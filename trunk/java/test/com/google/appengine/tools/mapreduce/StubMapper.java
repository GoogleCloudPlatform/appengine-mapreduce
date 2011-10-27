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

import com.google.appengine.tools.mapreduce.v2.impl.handlers.Worker;

import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * StubMapper is a Mapper that stores its state statically,
 * so when {@link Worker#handleMapperWorker(
 * javax.servlet.http.HttpServletRequest,
 * javax.servlet.http.HttpServletResponse)} creates it via reflection, we can
 * check it. This has the side effect that tests with the StubMapper cannot
 * be run in parallel, but them's the breaks.
 *
 *
 */
public class StubMapper extends
    AppEngineMapper<IntWritable, IntWritable, IntWritable, IntWritable> {
  // static because we want to test methods that create StubMappers
  public static List<IntWritable> invocationKeys = new ArrayList<IntWritable>();
  public static boolean setupCalled;
  public static boolean taskSetupCalled;
  public static boolean cleanupCalled;
  public static boolean taskCleanupCalled;

  private static void reset() {
    invocationKeys.clear();
    setupCalled = false;
    taskSetupCalled = false;
    cleanupCalled = false;
    taskCleanupCalled = false;
  }

  public static void fakeSetUp() {
    reset();
  }

  public static void fakeTearDown() {
    reset();
  }

  @Override
  public void map(IntWritable key, IntWritable value, Context context) {
    invocationKeys.add(key);
  }

  @Override
  public void setup(Context context) {
    setupCalled = true;
  }

  @Override
  public void taskSetup(Context context) {
    taskSetupCalled = true;
  }

  @Override
  public void cleanup(Context context) {
    cleanupCalled = true;
  }

  @Override
  public void taskCleanup(Context context) {
    taskCleanupCalled = true;
  }
}
