// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.LifecycleListener;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.Worker;
import com.google.appengine.tools.mapreduce.WorkerContext;
import com.google.appengine.tools.mapreduce.impl.handlers.MemoryLimiter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.IncrementalTask;
import com.google.appengine.tools.mapreduce.impl.shardedjob.RejectRequestException;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardFailureException;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values consumed by the worker
 * @param <O> type of output values produced by the worker
 * @param <C> type of context required by the worker
 */
public abstract class WorkerShardTask<I, O, C extends WorkerContext>
    implements IncrementalTask<WorkerShardTask<I, O, C>, WorkerResult<O>> {

  private static final Logger log = Logger.getLogger(WorkerShardTask.class.getName());
  private static final long serialVersionUID = 992552712402490981L;
  private static final MemoryLimiter LIMITER = new MemoryLimiter();

  protected final String mrJobId;
  protected final int shardNumber;
  protected final int shardCount;
  protected final InputReader<I> in;
  private final Worker<C> worker;
  protected final OutputWriter<O> out;
  private final String workerCallsCounterName;
  private final String workerMillisCounterName;
  private transient Stopwatch overallStopwatch;
  private transient Stopwatch inputStopwatch;
  private transient Stopwatch workerStopwatch;

  private boolean isFirstSlice = true;

  protected WorkerShardTask(String mrJobId, int shardNumber, int shardCount, InputReader<I> in,
      Worker<C> worker, OutputWriter<O> out, String workerCallsCounterName,
      String workerMillisCounterName) {
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.shardNumber = shardNumber;
    this.shardCount = shardCount;
    this.in = checkNotNull(in, "Null in");
    this.worker = checkNotNull(worker, "Null worker");
    this.out = checkNotNull(out, "Null out");
    this.workerCallsCounterName =
        checkNotNull(workerCallsCounterName, "Null workerCallsCounterName");
    this.workerMillisCounterName =
        checkNotNull(workerMillisCounterName, "Null workerMillisCounterName");
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "(" + mrJobId
        + ", " + shardNumber + "/" + shardCount + ")";
  }

  protected abstract C getWorkerContext(Counters counters);
  protected abstract void callWorker(I input);
  protected abstract String formatLastWorkItem(I item);

  /**
   * @return true iff a checkpoint should be performed.
   */
  protected abstract boolean shouldCheckpoint(long timeElapsed);
  protected abstract long estimateMemoryNeeded();

  @Override
  public RunResult<WorkerShardTask<I, O, C>, WorkerResult<O>> run() {
    long claimedMemory = LIMITER.claim(estimateMemoryNeeded() / 1024 / 1024);
    try {
      return doWork();
    } catch (RecoverableException | ShardFailureException | RejectRequestException ex) {
      throw ex;
    } catch (RuntimeException ex) {
      throw new ShardFailureException(shardNumber, ex);
    } finally {
      LIMITER.release(claimedMemory);
    }
  }

  public RunResult<WorkerShardTask<I, O, C>, WorkerResult<O>> doWork() {
    try {
      beginSlice();
    } catch (RecoverableException | ShardFailureException | RejectRequestException ex) {
      throw ex;
    } catch (IOException | RuntimeException ex) {
      throw new RecoverableException("Failed on beginSlice/beginShard", ex);
    }
    overallStopwatch = Stopwatch.createStarted();
    inputStopwatch =  Stopwatch.createUnstarted();
    workerStopwatch = Stopwatch.createUnstarted();

    int workerCalls = 0;
    int itemsRead = 0;
    boolean inputExhausted = false;
    I next = null;
    try {
      do {
        inputStopwatch.start();
        try {
          next = in.next();
          itemsRead++;
        } catch (NoSuchElementException e) {
          inputExhausted = true;
          break;
        } catch (IOException e) {
          if (workerCalls == 0) {
            // No progress, lets retry the slice
            throw new RecoverableException("Failed on first input", e);
          }
          // We made progress, persist it.
          log.log(Level.WARNING, "An IOException while reading input, ending slice early", e);
          break;
        }
        inputStopwatch.stop();

        workerCalls++;
        // TODO(ohler): workerStopwatch includes time spent in emit() and the
        // OutputWriter, which is very significant because it includes I/O. I
        // think a clean solution would be to have the context measure the
        // OutputWriter's time, and to subtract that from the time measured by
        // workerStopwatch.
        workerStopwatch.start();
        // TODO(user): add a way to check if the writer is OK with a slice-retry
        // and if so, wrap callWorker with try~catch and propagate as RecoverableException.
        // Otherwise should be propagated as ShardFailureException and remove the
        // individuals try~catch in the callWorker implementations.
        callWorker(next);
        workerStopwatch.stop();
      } while (!shouldCheckpoint(overallStopwatch.elapsed(MILLISECONDS)));
    } finally {
      log.info("Ending slice after " + itemsRead + " items read and calling the worker "
          + workerCalls + " times");
    }
    overallStopwatch.stop();
    log.info("Ending slice, inputExhausted=" + inputExhausted + ", overallStopwatch="
        + overallStopwatch + ", workerStopwatch=" + workerStopwatch + ", inputStopwatch="
        + inputStopwatch);
    Counters counters = worker.getContext().getCounters();
    counters.getCounter(workerCallsCounterName).increment(workerCalls);
    counters.getCounter(workerMillisCounterName).increment(workerStopwatch.elapsed(MILLISECONDS));

    try {
      endSlice(inputExhausted);
    } catch (IOException | RuntimeException ex) {
      // TODO(user): similar to callWorker, if writer has a way to indicate that a slice-retry
      // is OK we should consider a broader catch and possibly throwing RecoverableException
      throw new ShardFailureException(shardNumber, "Failed on endSlice/endShard", ex);
    }

    WorkerShardState workerShardState =
        new WorkerShardState(workerCalls, System.currentTimeMillis(), formatLastWorkItem(next));
    if (!inputExhausted) {
      return RunResult.of(new WorkerResult<O>(shardNumber, workerShardState, counters), this);
    } else {
      return RunResult.<WorkerShardTask<I, O, C>, WorkerResult<O>>of(
          new WorkerResult<O>(shardNumber, out, workerShardState, counters), null);
    }
  }

  private void beginSlice() throws IOException {
    if (isFirstSlice) {
      out.open();
      in.open();
    }
    in.beginSlice();
    out.beginSlice();
    Counters counters = new CountersImpl();
    C context = getWorkerContext(counters);
    worker.setContext(context);
    if (isFirstSlice) {
      worker.beginShard();
    }
    for (LifecycleListener listener : worker.getLifecycleListenerRegistry().getListeners()) {
      listener.beginSlice();
    }
    worker.beginSlice();
    isFirstSlice = false;
  }

  private void endSlice(boolean inputExhausted) throws IOException {
    worker.endSlice();
    if (inputExhausted) {
      worker.endShard();
    }
    for (LifecycleListener listener :
        Lists.reverse(worker.getLifecycleListenerRegistry().getListeners())) {
      listener.endSlice();
    }
    worker.setContext(null);
    out.endSlice();
    in.endSlice();
    if (inputExhausted) {
      out.close();
      try {
        in.close();
      } catch (IOException ex) {
        // Ignore - retrying a slice or shard will not fix that
      }
    }
  }

  protected static String abbrev(Object x) {
    if (x == null) {
      return null;
    }
    String s = x.toString();
    if (s.length() > MapReduceConstants.MAX_LAST_ITEM_STRING_SIZE) {
      return s.substring(0, MapReduceConstants.MAX_LAST_ITEM_STRING_SIZE) + "...";
    } else {
      return s;
    }
  }
}
