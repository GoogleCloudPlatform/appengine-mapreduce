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

import com.google.appengine.api.datastore.DatastoreServiceFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.IOException;

/**
 * An AppEngineMapper is a Hadoop Mapper that is run via a sequential
 * series of task queue executions.
 *
 * <p>As such, the  {@link #run(org.apache.hadoop.mapreduce.Mapper.Context)}
 * method is unusable (since task state doesn't persist from one queue iteration
 * to the next).
 *
 * <p>Additionally, the {@link Mapper} interface is extended with two methods
 * that get executed with each task queue invocation:
 * {@link #taskSetup(org.apache.hadoop.Mapper.Context)} and
 * {@link #taskCleanup(org.apache.hadoop.Mapper.Context)}.
 *
 * <p>The {@link Context} object that is passed to each of the AppEngineMapper
 * methods is actually an {@link AppEngineContext} object. Therefore, you can
 * access an automatically flushed {@link DatastoreMutationPool} via the
 * {@link AppEngineContext#getMutationPool()} method. Note: For the automatic
 * flushing behavior, you must call
 * {@link #taskCleanup(org.apache.hadoop.Mapper.Context)} if you override that
 * method in a subclass.
 *
 */
public abstract class AppEngineMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

  /**
   * A Context that holds a datastore mutation pool.
   */
  public class AppEngineContext extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>.Context {
    private DatastoreMutationPool mutationPool;
    private int datastoreMutationPoolCountLimit = DatastoreMutationPool.DEFAULT_COUNT_LIMIT;
    private int datastoreMutationPoolSizeLimit = DatastoreMutationPool.DEFAULT_SIZE_LIMIT;

    public AppEngineContext(Configuration conf,
                            TaskAttemptID taskid,
                            RecordReader<KEYIN, VALUEIN> reader,
                            RecordWriter<KEYOUT, VALUEOUT> writer,
                            OutputCommitter committer,
                            StatusReporter reporter,
                            InputSplit split) throws IOException, InterruptedException {
      super(conf, taskid, reader, writer, committer, reporter, split);
    }

    /**
     * Sets the number of mutations and the accumulated size of the mutations
     * before the mutation pool is flushed. May only be called before the first
     * call to {@link #getMutationPool()}.
     *
     * @param countLimit the number of mutations to collect before the mutation
     * pool is flushed
     * @param sizeLimit the accumulated size of mutations (in bytes) to collect
     * before the mutation pool is flushed
     * @throws IllegalStateException if {@link #getMutationPool()} has already
     * been called
     */
    public void setMutationPoolFlushParameters(int countLimit, int sizeLimit) {
      if (mutationPool != null) {
        throw new IllegalStateException("setMutationPoolFlushParameters() may only "
            + "be called before any calls to getMutationPool()");
      }
      datastoreMutationPoolCountLimit = countLimit;
      datastoreMutationPoolSizeLimit = sizeLimit;
    }

    public DatastoreMutationPool getMutationPool() {
      if (mutationPool == null) {
        mutationPool = new DatastoreMutationPool(DatastoreServiceFactory.getDatastoreService(),
            datastoreMutationPoolCountLimit, datastoreMutationPoolSizeLimit);
      }
      return mutationPool;
    }

    public void flush() {
      if (mutationPool != null) {
        mutationPool.flush();
      }
    }
  }

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
   * Run at the end of each task queue invocation. The default flushes the context.
   */
  public void taskCleanup(Context context) throws IOException, InterruptedException {
    getAppEngineContext(context).flush();
  }

  @Override
  public void map(KEYIN key, VALUEIN value, Context context)
      throws IOException, InterruptedException {
    // Nothing (super does the identity map function, which is a bad idea since
    // we don't support shuffle/reduce yet).
  }

  /**
   * Get an {@code AppEngineContext} from the {@code Context} that's passed
   * to the other {@code AppEngineMapper} methods.
   */
  public AppEngineContext getAppEngineContext(Context context) {
    // We're the only client of the other methods, so we know that the context
    // will really be an AppEngineContext.
    return (AppEngineContext) context;
  }
}
