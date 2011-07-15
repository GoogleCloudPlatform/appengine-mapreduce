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

package com.google.appengine.demos.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.AppEngineMapper;

import org.apache.hadoop.io.NullWritable;

import java.util.logging.Logger;

/**
 * A sample AppEngine mapper.
 *
 */
public class TestMapper extends AppEngineMapper<Key, Entity, NullWritable, NullWritable> {
  private static final Logger log = Logger.getLogger(TestMapper.class.getName());

  public TestMapper() {
  }

  @Override
  public void taskSetup(Context context) {
    log.warning("Doing per-task setup");
  }

  @Override
  public void taskCleanup(Context context) {
    log.warning("Doing per-task cleanup");
  }

  @Override
  public void setup(Context context) {
    log.warning("Doing per-worker setup");
  }

  @Override
  public void cleanup(Context context) {
    log.warning("Doing per-worker cleanup");
  }

  // This is a silly mapper that's intended to show some of the capabilities of the API.
  @Override
  public void map(Key key, Entity value, Context context) {
    log.warning("Mapping key: " + key);
    if (value.hasProperty("skub")) {
      // Counts the number of jibbit and non-jibbit skub.
      // These counts are aggregated and can be seen on the status page.
      if (value.getProperty("skub").equals("Pro")) {
        context.getCounter("Skub", "Pro").increment(1);
      } else {
        context.getCounter("Skub", "Anti").increment(1);
      }
    }
  }
}
