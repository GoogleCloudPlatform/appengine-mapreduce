package com.google.appengine.tools.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progress;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;


/**
 * An AppEngineReducer is a Hadoop Reducer that is run via a sequential
 * series of task queue executions.
 *
 * <p>As such, the  {@link #run(org.apache.hadoop.mapreduce.Reducer.Context)}
 * method is unusable (since task state doesn't persist from one queue iteration
 * to the next).
 *
 * <p>Additionally, the {@link org.apache.hadoop.mapreduce.Mapper} interface is extended with two
 * methods that get executed with each task queue invocation:
 * {@link #taskSetup(org.apache.hadoop.mapreduce.Reducer.Context)} and
 * {@link #taskCleanup(org.apache.hadoop.mapreduce.Reducer.Context)}.
 *
 */
public class AppEngineReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
  @Override
  public final void run(Context context) throws IOException, InterruptedException {
    throw new UnsupportedOperationException("AppEngineMappers don't have run methods");
  }

  @Override
  public void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
      throws IOException, InterruptedException {

  }

  public class AppEngineReducerContext extends Context {
    // FYI: called only via reflection
    public AppEngineReducerContext(Configuration configuration,
                                   TaskAttemptID taskAttemptID,
                                   RawKeyValueIterator rawKeyValueIterator,
                                   Counter inputKeyCounter,
                                   Counter inputValueCounter,
                                   RecordWriter<KEYOUT, VALUEOUT> writer,
                                   OutputCommitter committer,
                                   StatusReporter reporter,
                                   RawComparator<KEYIN> comparator,
                                   Class<KEYIN> keyClass,
                                   Class<VALUEIN> valueClass)
        throws IOException, InterruptedException {

      super(configuration,
          taskAttemptID,
          rawKeyValueIterator,
          inputKeyCounter,
          inputValueCounter,
          writer,
          committer,
          reporter,
          comparator,
          keyClass,
          valueClass);
    }

    @Override
    public final boolean nextKey() throws IOException, InterruptedException {
      throw new UnsupportedOperationException("AppEngineReducer doesn't support nextKey()");
    }
  }

  /**
   * The Hadoop ReducerContext implementation calls the next() method upon construction so we need
   * to pass a non-null value in.
   */
  static class NullRawKeyValueIterator implements RawKeyValueIterator {
    @Override
    public DataInputBuffer getKey() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DataInputBuffer getValue() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean next() throws IOException {
      return false;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Progress getProgress() {
      throw new UnsupportedOperationException();
    }
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
    // nothing
  }

  static AppEngineReducer.AppEngineReducerContext getReducerContext(
      AppEngineReducer reducer,
      JobContext context,
      RecordWriter recordWriter,
      OutputCommitter outputCommitter,
      Configuration configuration,
      TaskAttemptID taskAttemptID) throws IOException, InterruptedException {
    Constructor<AppEngineReducer.AppEngineReducerContext> contextConstructor;
    try {
      contextConstructor = AppEngineReducer.AppEngineReducerContext.class.getConstructor(
          new Class[]{
              AppEngineReducer.class,
              Configuration.class,
              TaskAttemptID.class,
              RawKeyValueIterator.class,
              Counter.class,
              Counter.class,
              RecordWriter.class,
              OutputCommitter.class,
              StatusReporter.class,
              RawComparator.class,
              Class.class,
              Class.class });

      Counters counters = new Counters();

      return
          contextConstructor.newInstance(
              reducer,
              configuration,
              taskAttemptID,
              new AppEngineReducer.NullRawKeyValueIterator(),
              counters.findCounter("null", "null"),
              counters.findCounter("null", "null"),
              recordWriter,
              outputCommitter,
              null,  // StatusReporter
              null,  // RawComparator
              // This will fail if the class is not an instance of Writable.class
              LongWritable.class,
              context.getMapOutputValueClass());

    } catch (SecurityException e) {
      // Since we know the class we're calling, this is strictly a programming error.
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    } catch (NoSuchMethodException e) {
      // Same
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    } catch (IllegalArgumentException e) {
      // There's a small chance this could be a bad supplied argument,
      // but we should validate that earlier.
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    } catch (InstantiationException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    } catch (IllegalAccessException e) {
      // Programming error
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException("Couldn't initialize Reducer.Context", e);
    }
  }
}
