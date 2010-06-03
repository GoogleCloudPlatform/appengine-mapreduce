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

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * An AppEngineMapper is a Hadoop Mapper that is run via a sequential
 * series of task queue executions.
 * 
 * <p>As such, the  {@link #run(org.apache.hadoop.mapreduce.Mapper.Context)}
 * method is unusable (since task state doesn't persist from one queue iteration
 * to the next).
 * 
 * <p>Additionally, the {@link Mapper} interface is extended with two methods that
 * get executed with each task queue invocation: 
 * {@link #taskSetup(org.apache.hadoop.Mapper.Context)} and 
 * {@link #taskCleanup(org.apache.hadoop.Mapper.Context)}. 
 * 
 * @author frew@google.com (Fred Wulff)
 */
public abstract class AppEngineMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> 
    extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  
  /**
   * App Engine mappers have no {@code run(Context)} method, since it would
   * have to span multiple task queue invocations. Therefore, calling this
   * method always throws {@link java.lang.UnsupportedOperationException}.
   * 
   * @throws UnsupportedOperationException always
   */
  @Override
  public final void run(Context context) {
    throw new UnsupportedOperationException("AppEngineMappers don't have run methods");
  }
  
  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    // Nothing
  }
  
  /**
   * Run at the start of each task queue invocation.
   */
  public void taskSetup(Context context) throws IOException, InterruptedException {
    // Nothing
  }
  
  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    // Nothing
  }
  
  /**
   * Run at the end of each task queue invocation.
   */
  public void taskCleanup(Context context) throws IOException, InterruptedException {
    // Nothing
  }
  
  @Override
  public void map(KEYIN key, VALUEIN value, Context context) 
      throws IOException, InterruptedException {
    // Nothing (super does the identity map function, which is a bad idea since
    // we don't support shuffle/reduce yet).
  }
}
