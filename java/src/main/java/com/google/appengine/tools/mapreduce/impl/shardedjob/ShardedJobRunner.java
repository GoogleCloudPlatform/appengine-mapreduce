// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.cloudstorage.RetryHelper.runWithRetries;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ABORTED;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ERROR;
import static java.util.concurrent.Executors.callable;

import com.google.appengine.api.datastore.CommittedButStillApplyingException;
import com.google.appengine.api.datastore.DatastoreFailureException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.DatastoreTimeoutException;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.log.LogQuery;
import com.google.appengine.api.log.LogService;
import com.google.appengine.api.log.LogServiceException;
import com.google.appengine.api.log.LogServiceFactory;
import com.google.appengine.api.log.RequestLogs;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TransactionalTaskException;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.DeleteShardedJob;
import com.google.appengine.tools.mapreduce.impl.shardedjob.pipeline.FinalizeShardedJob;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.ArgumentException;
import com.google.apphosting.api.ApiProxy.RequestTooLargeException;
import com.google.apphosting.api.ApiProxy.ResponseTooLargeException;
import com.google.apphosting.api.DeadlineExceededException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains all logic to manage and run sharded jobs.
 *
 * This is a helper class for {@link ShardedJobServiceImpl} that implements
 * all the functionality but assumes fixed types for {@code <T>}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of tasks that the job being processed consists of
 */
public class ShardedJobRunner<T extends IncrementalTask> implements ShardedJobHandler {

  // High-level overview:
  //
  // A sharded job is started with a given number of tasks, task is invoked
  // over and over until it indicates it is complete.
  //
  // Each task is its own entity group to avoid contention.
  //
  // There is also a single entity (in its own entity group) that holds the
  // overall job state. It is updated only during initialization and when the tasks complete.
  //
  // Tasks entities carry a "sequence number" that allows it to detect if its work has already
  // been done (useful in case the task queue runs it twice). We schedule each
  // task in the same datastore transaction that updates the sequence number in
  // the entity.
  //
  // Each task also checks the job state entity to detect if the job has been
  // aborted or deleted, and terminates if so.


  private static final Logger log = Logger.getLogger(ShardedJobRunner.class.getName());

  static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();
  private static final LogService LOG_SERVICE = LogServiceFactory.getLogService();

  private static final RetryParams DATASTORE_RETRY_PARAMS = new RetryParams.Builder()
      .initialRetryDelayMillis(1000).maxRetryDelayMillis(30000).retryMinAttempts(5).build();

  public static final RetryParams DATASTORE_RETRY_FOREVER_PARAMS =
      new RetryParams.Builder(DATASTORE_RETRY_PARAMS)
          .retryMaxAttempts(Integer.MAX_VALUE)
          .totalRetryPeriodMillis(Long.MAX_VALUE)
          .build();

  public static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
      ApiProxyException.class, ConcurrentModificationException.class,
      DatastoreFailureException.class, CommittedButStillApplyingException.class,
      DatastoreTimeoutException.class, TransientFailureException.class,
      TransactionalTaskException.class).abortOn(RequestTooLargeException.class,
      ResponseTooLargeException.class, ArgumentException.class).build();

  private static final ExceptionHandler AGGRESIVE_EXCEPTION_HANDLER = new ExceptionHandler.Builder()
      .retryOn(Exception.class).abortOn(RequestTooLargeException.class,
          ResponseTooLargeException.class, ArgumentException.class,
          DeadlineExceededException.class).build();

  private ShardedJobStateImpl<T> lookupJobState(Transaction tx, String jobId) {
    try {
      Entity entity = DATASTORE.get(tx, ShardedJobStateImpl.ShardedJobSerializer.makeKey(jobId));
      return ShardedJobStateImpl.ShardedJobSerializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  @VisibleForTesting
  IncrementalTaskState<T> lookupTaskState(Transaction tx, String taskId) {
    try {
      Entity entity = DATASTORE.get(tx, IncrementalTaskState.Serializer.makeKey(taskId));
      return IncrementalTaskState.Serializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  @VisibleForTesting
  ShardRetryState<T> lookupShardRetryState(String taskId) {
    try {
      Entity entity = DATASTORE.get(ShardRetryState.Serializer.makeKey(taskId));
      return ShardRetryState.Serializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  Iterator<IncrementalTaskState<T>> lookupTasks(
      final String jobId, final int taskCount, final boolean lenient) {
    return new AbstractIterator<IncrementalTaskState<T>>() {
      private int lastCount;
      private Iterator<Entity> lastBatch = Collections.emptyIterator();

      @Override
      protected IncrementalTaskState<T> computeNext() {
        if (lastBatch.hasNext()) {
          Entity entity = lastBatch.next();
          return IncrementalTaskState.Serializer.<T>fromEntity(entity, lenient);
        } else if (lastCount >= taskCount) {
          return endOfData();
        }
        int toRead = Math.min(20, taskCount - lastCount);
        List<Key> keys = new ArrayList<>(toRead);
        for (int i = 0; i < toRead; i++, lastCount++) {
          Key key = IncrementalTaskState.Serializer.makeKey(getTaskId(jobId, lastCount));
          keys.add(key);
        }
        TreeMap<Integer, Entity> ordered = new TreeMap<>();
        for (Entry<Key, Entity> entry : DATASTORE.get(keys).entrySet()) {
          ordered.put(parseTaskNumberFromTaskId(jobId, entry.getKey().getName()), entry.getValue());
        }
        lastBatch = ordered.values().iterator();
        return computeNext();
      }
    };
  }

  private void callCompleted(ShardedJobStateImpl<T> jobState) {
    Iterator<IncrementalTaskState<T>> taskStates =
        lookupTasks(jobState.getJobId(), jobState.getTotalTaskCount(), false);
    Iterator<T> tasks = Iterators.transform(taskStates, new Function<IncrementalTaskState<T>, T>() {
      @Override public T apply(IncrementalTaskState<T> taskState) {
        return taskState.getTask();
      }
    });
    jobState.getController().completed(tasks);
  }

  private void scheduleControllerTask(Transaction tx, String jobId, String taskId,
      ShardedJobSettings settings) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getControllerPath())
        .param(JOB_ID_PARAM, jobId)
        .param(TASK_ID_PARAM, taskId);
    taskOptions.header("Host", settings.getTaskQueueTarget());
    QueueFactory.getQueue(settings.getQueueName()).add(tx, taskOptions);
  }

  private void scheduleWorkerTask(Transaction tx, ShardedJobSettings settings,
      IncrementalTaskState<T> state, Long eta) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getWorkerPath())
        .param(TASK_ID_PARAM, state.getTaskId())
        .param(JOB_ID_PARAM, state.getJobId())
        .param(SEQUENCE_NUMBER_PARAM, String.valueOf(state.getSequenceNumber()));
    taskOptions.header("Host", settings.getTaskQueueTarget());
    if (eta != null) {
      taskOptions.etaMillis(eta);
    }
    QueueFactory.getQueue(settings.getQueueName()).add(tx, taskOptions);
  }

  @Override
  public void completeShard(final String jobId, final String taskId) {
    log.info("Polling task states for job " + jobId);
    final int shardNumber = parseTaskNumberFromTaskId(jobId, taskId);
    ShardedJobStateImpl<T> jobState = runWithRetries(new Callable<ShardedJobStateImpl<T>>() {
      @Override
      public ShardedJobStateImpl<T> call() throws ConcurrentModificationException,
          DatastoreFailureException {
        Transaction tx = DATASTORE.beginTransaction();
        try {
          ShardedJobStateImpl<T> jobState = lookupJobState(tx, jobId);
          if (jobState == null) {
            return null;
          }
          jobState.setMostRecentUpdateTimeMillis(
              Math.max(System.currentTimeMillis(), jobState.getMostRecentUpdateTimeMillis()));
          jobState.markShardCompleted(shardNumber);

          if (jobState.getActiveTaskCount() == 0 && jobState.getStatus().isActive()) {
            jobState.setStatus(new Status(DONE));
          }
          DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));
          tx.commit();
          return jobState;
        } finally {
          rollbackIfActive(tx);
        }
      }
    }, DATASTORE_RETRY_PARAMS, EXCEPTION_HANDLER);

    if (jobState == null) {
      log.info(taskId + ": Job is gone, ignoring completeShard call.");
      return;
    }

    if (jobState.getActiveTaskCount() == 0) {
      if (jobState.getStatus().getStatusCode() == DONE) {
        log.info("Calling completed for " + jobId);
        // TODO(user): consider trying failed if completed failed after N attempts
        callCompleted(jobState);
      } else {
        log.info("Calling failed for " + jobId + ", status=" + jobState.getStatus());
        jobState.getController().failed(jobState.getStatus());
      }
      PipelineService pipeline = PipelineServiceFactory.newPipelineService();
      pipeline.startNewPipeline(
          new FinalizeShardedJob(jobId, jobState.getTotalTaskCount(), jobState.getStatus()));
    }
  }

  private IncrementalTaskState<T> getAndValidateTaskState(Transaction tx, String taskId,
      int sequenceNumber, ShardedJobStateImpl<T> jobState) {
    IncrementalTaskState<T> taskState = lookupTaskState(tx, taskId);
    if (taskState == null) {
      log.warning(taskId + ": Task gone");
      return null;
    }
    if (!taskState.getStatus().isActive()) {
      log.info(taskId + ": Task no longer active: " + taskState);
      return null;
    }
    if (!jobState.getStatus().isActive()) {
      taskState.setStatus(new Status(StatusCode.ABORTED));
      log.info(taskId + ": Job no longer active: " + jobState + ", aborting task.");
      updateTask(jobState, taskState, null, false);
      return null;
    }
    if (sequenceNumber == taskState.getSequenceNumber()) {
      if (!taskState.getLockInfo().isLocked()) {
        return taskState;
      }
      handleLockHeld(taskId, jobState, taskState);
    } else {
      if (taskState.getSequenceNumber() > sequenceNumber) {
        log.info(taskId + ": Task sequence number " + sequenceNumber + " already completed: "
            + taskState);
      } else {
        log.severe(taskId + ": Task state is from the past: " + taskState);
      }
    }
    return null;
  }

  /**
   * Handle a locked slice case.
   */
  private void handleLockHeld(String taskId, ShardedJobStateImpl<T> jobState,
      IncrementalTaskState<T> taskState) {
    long currentTime = System.currentTimeMillis();
    int sliceTimeoutMillis = jobState.getSettings().getSliceTimeoutMillis();
    long lockExpiration = taskState.getLockInfo().lockedSince() + sliceTimeoutMillis;
    boolean wasRequestCompleted = wasRequestCompleted(taskState.getLockInfo().getRequestId());

    if (lockExpiration > currentTime && !wasRequestCompleted) {
      // if lock was not expired AND not abandon reschedule in 1 minute.
      long eta = Math.min(lockExpiration, currentTime + 60_000);
      scheduleWorkerTask(null, jobState.getSettings(), taskState, eta);
      log.info("Lock for " + taskId + " is being held. Will retry after " + (eta - currentTime));
    } else {
      ShardRetryState<T> retryState;
      if (wasRequestCompleted) {
        retryState = handleSliceFailure(jobState, taskState, new RuntimeException(
            "Resuming after abandon lock for " + taskId + " on slice: "
                + taskState.getSequenceNumber()), true);
      } else {
        retryState = handleShardFailure(jobState, taskState, new RuntimeException(
          "Lock for " + taskId + " expired on slice: " + taskState.getSequenceNumber()));
      }
      updateTask(jobState, taskState, retryState, false);
    }
  }

  private static boolean wasRequestCompleted(String requestId) {
    if (requestId != null) {
      LogQuery query = LogQuery.Builder.withRequestIds(Collections.singletonList(requestId));
      try {
        for (RequestLogs requestLog : LOG_SERVICE.fetch(query)) {
          if (requestLog.isFinished()) {
            log.info("Previous un-released lock for request " + requestId + " has finished");
            return true;
          }
        }
      } catch (LogServiceException ex) {
        // consider any log-fetch failure as if request is not known to be completed
        log.log(Level.FINE, "Failed to query log service for request " + requestId, ex);
      }
    }
    return false;
  }

  private boolean lockShard(Transaction tx, ShardedJobStateImpl<T> jobState,
      IncrementalTaskState<T> taskState) {
    boolean locked = false;
    taskState.getLockInfo().lock();
    Entity entity = IncrementalTaskState.Serializer.toEntity(tx, taskState);
    try {
      DATASTORE.put(tx, entity);
      tx.commit();
      locked = true;
    } catch (ConcurrentModificationException ex) {
      // TODO: would be nice to have a test for this...
      log.warning("Failed to acquire the lock, Will reschedule task for: " + taskState.getJobId()
          + " on slice " + taskState.getSequenceNumber());
      long eta = System.currentTimeMillis() + new Random().nextInt(5000) + 5000;
      scheduleWorkerTask(null, jobState.getSettings(), taskState, eta);
    } finally {
      if (!locked) {
        taskState.getLockInfo().unlock();
      }
    }
    return locked;
  }

  @Override
  public void runTask(final String jobId, final String taskId, final int sequenceNumber) {
    final ShardedJobStateImpl<T> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      log.info(taskId + ": Job is gone, ignoring runTask call.");
      return;
    }
    Transaction tx = DATASTORE.beginTransaction();
    try {
      IncrementalTaskState<T> taskState =
          getAndValidateTaskState(tx, taskId, sequenceNumber, jobState);
      if (taskState == null) {
        return;
      }
      T task = taskState.getTask();
      task.prepare();
      try {
        if (lockShard(tx, jobState, taskState)) {
          runAndUpdateTask(jobId, taskId, sequenceNumber, jobState, taskState);
        }
      } finally {
        task.cleanup();
      }
    } finally {
      rollbackIfActive(tx);
    }
  }

  private void runAndUpdateTask(final String jobId, final String taskId, final int sequenceNumber,
      final ShardedJobStateImpl<T> jobState, IncrementalTaskState<T> taskState) {
    ShardRetryState<T> retryState = null;
    try {
      String statusUrl = jobState.getSettings().getPipelineStatusUrl();
      log.info("Running task " + taskId + " (job " + jobId + "), sequence number " + sequenceNumber
          + (statusUrl != null ? " Progress can be monitored at: " + statusUrl : ""));
      T task = taskState.getTask();
      task.run();
      if (task.isDone()) {
        taskState.setStatus(new Status(StatusCode.DONE));
      }
      taskState.clearRetryCount();
      taskState.setMostRecentUpdateMillis(System.currentTimeMillis());
    } catch (ShardFailureException ex) {
      retryState = handleShardFailure(jobState, taskState, ex);
    } catch (JobFailureException ex) {
      log.log(Level.WARNING,
          "Shard " + taskState.getTaskId() + " triggered job failure", ex);
      handleJobFailure(taskState, ex);
    } catch (RuntimeException ex) {
      retryState = handleSliceFailure(jobState, taskState, ex, false);
    } catch (Throwable ex) {
      log.log(Level.WARNING, "Slice encountered an Error.");
      retryState = handleShardFailure(jobState, taskState, new RuntimeException("Error", ex));
    }

    try {
      updateTask(jobState, taskState, retryState, true);
    } catch (RetryHelperException ex) {
      log.severe("Failed to write end of slice for task: " + taskState.getTask());
      // TODO(user): consider what to do here when this fail (though options are limited)
      throw ex;
    }
  }

  private ShardRetryState<T> handleSliceFailure(ShardedJobStateImpl<T> jobState,
      IncrementalTaskState<T> taskState, RuntimeException ex, boolean abandon) {
    if (!(ex instanceof RecoverableException) && !taskState.getTask().allowSliceRetry(abandon)) {
      return handleShardFailure(jobState, taskState, ex);
    }
    int attempts = taskState.incrementAndGetRetryCount();
    if (attempts > jobState.getSettings().getMaxSliceRetries()){
      log.log(Level.WARNING, "Slice exceeded its max attempts.");
      return handleShardFailure(jobState, taskState, ex);
    } else {
      log.log(Level.INFO, "Slice attempt #" + attempts + " failed. Going to retry.", ex);
    }
    return null;
  }

  private ShardRetryState<T> handleShardFailure(ShardedJobStateImpl<T> jobState,
      IncrementalTaskState<T> taskState, Exception ex) {
    ShardRetryState<T> retryState = lookupShardRetryState(taskState.getTaskId());
    if (retryState.incrementAndGet() > jobState.getSettings().getMaxShardRetries()) {
      log.log(Level.SEVERE, "Shard exceeded its max attempts, setting job state to ERROR.", ex);
      handleJobFailure(taskState, ex);
    } else {
      log.log(Level.INFO,
          "Shard attempt #" + retryState.getRetryCount() + " failed. Going to retry.", ex);
      taskState.setTask(retryState.getInitialTask());
      taskState.clearRetryCount();
    }
    return retryState;
  }

  private void handleJobFailure(IncrementalTaskState<T> taskState, Exception ex) {
    changeJobStatus(taskState.getJobId(), new Status(ERROR, ex));
    taskState.setStatus(new Status(StatusCode.ERROR, ex));
    taskState.incrementAndGetRetryCount(); // trigger saving the last task instead of current
  }

  private void updateTask(final ShardedJobStateImpl<T> jobState,
      final IncrementalTaskState<T> taskState, /* Nullable */
      final ShardRetryState<T> shardRetryState, boolean aggresiveRetry) {
    taskState.setSequenceNumber(taskState.getSequenceNumber() + 1);
    taskState.getLockInfo().unlock();
    ExceptionHandler exceptionHandler =
        aggresiveRetry ? AGGRESIVE_EXCEPTION_HANDLER : EXCEPTION_HANDLER;
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override
      public void run() {
        Transaction tx = DATASTORE.beginTransaction();
        try {
          String taskId = taskState.getTaskId();
          IncrementalTaskState<T> existing = lookupTaskState(tx, taskId);
          if (existing == null) {
            log.info(taskId + ": Ignoring an update, as task disappeared while processing");
          } else if (existing.getSequenceNumber() != taskState.getSequenceNumber() - 1) {
            log.warning(taskId + ": Ignoring an update, a concurrent execution changed it to: "
                + existing);
          } else {
            if (existing.getRetryCount() < taskState.getRetryCount()) {
              // Slice retry, we need to reset state
              taskState.setTask(existing.getTask());
            }
            writeTaskState(taskState, shardRetryState, tx);
            scheduleTask(jobState, taskState, tx);
            tx.commit();
          }
        } finally {
          rollbackIfActive(tx);
        }
      }

      private void writeTaskState(IncrementalTaskState<T> taskState,
          ShardRetryState<T> shardRetryState, Transaction tx) {
        Entity taskStateEntity = IncrementalTaskState.Serializer.toEntity(tx, taskState);
        if (shardRetryState == null) {
          DATASTORE.put(tx, taskStateEntity);
        } else {
          Entity retryStateEntity = ShardRetryState.Serializer.toEntity(tx, shardRetryState);
          DATASTORE.put(tx, Arrays.asList(taskStateEntity, retryStateEntity));
        }
      }

      private void scheduleTask(ShardedJobStateImpl<T> jobState,
          IncrementalTaskState<T> taskState, Transaction tx) {
        if (taskState.getStatus().isActive()) {
          scheduleWorkerTask(tx, jobState.getSettings(), taskState, null);
        } else {
          scheduleControllerTask(tx, jobState.getJobId(), taskState.getTaskId(),
              jobState.getSettings());
        }
      }
    }), DATASTORE_RETRY_FOREVER_PARAMS, exceptionHandler);
  }

  public static String getTaskId(String jobId, int taskNumber) {
    return jobId + "-task-" + taskNumber;
  }

  private static int parseTaskNumberFromTaskId(String jobId, String taskId) {
    String prefix = jobId + "-task-";
    if (!taskId.startsWith(prefix)) {
      throw new IllegalArgumentException("Invalid taskId: " + taskId);
    }
    return Integer.parseInt(taskId.substring(prefix.length()));
  }

  private void createTasks(ShardedJobSettings settings, String jobId,
      List<? extends T> initialTasks, long startTimeMillis) {
    log.info(jobId + ": Creating " + initialTasks.size() + " tasks");
    int id = 0;
    for (T initialTask : initialTasks) {
      // TODO(user): shardId (as known to WorkerShardTask) and taskId happen to be the same
      // number, just because they are created in the same order and happen to use their ordinal.
      // We should have way to inject the "shard-id" to the task.
      String taskId = getTaskId(jobId, id++);
      Transaction tx = DATASTORE.beginTransaction();
      try {
        IncrementalTaskState<T> taskState = lookupTaskState(tx, taskId);
        if (taskState != null) {
          log.info(jobId + ": Task already exists: " + taskState);
          continue;
        }
        taskState = IncrementalTaskState.<T>create(taskId, jobId, startTimeMillis, initialTask);
        ShardRetryState<T> retryState = ShardRetryState.createFor(taskState);
        DATASTORE.put(tx, Arrays.asList(IncrementalTaskState.Serializer.toEntity(tx, taskState),
            ShardRetryState.Serializer.toEntity(tx, retryState)));
        scheduleWorkerTask(tx, settings, taskState, null);
        tx.commit();
      } finally {
        rollbackIfActive(tx);
      }
    }
  }

  private void writeInitialJobState(ShardedJobStateImpl<T> jobState) {
    String jobId = jobState.getJobId();
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobStateImpl<T> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));
        tx.commit();
        log.info(jobId + ": Writing initial job state");
      } else {
        log.info(jobId + ": Ignoring Attempt to reinitialize job state: " + existing);
      }
    } finally {
      rollbackIfActive(tx);
    }
  }

  void startJob(final String jobId, List<? extends T> initialTasks,
      ShardedJobController<T> controller, ShardedJobSettings settings) {
    long startTime = System.currentTimeMillis();
    Preconditions.checkArgument(!Iterables.any(initialTasks, Predicates.isNull()),
        "Task list must not contain null values");
    ShardedJobStateImpl<T> jobState =
        ShardedJobStateImpl.create(jobId, controller, settings, initialTasks.size(), startTime);
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      jobState.setStatus(new Status(DONE));
      DATASTORE.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, jobState));
      controller.completed(Collections.<T>emptyIterator());
    } else {
      writeInitialJobState(jobState);
      createTasks(settings, jobId, initialTasks, startTime);
      log.info(jobId + ": All tasks were created");
    }
  }

  ShardedJobState getJobState(String jobId) {
    try {
      Entity entity = DATASTORE.get(null, ShardedJobStateImpl.ShardedJobSerializer.makeKey(jobId));
      return ShardedJobStateImpl.ShardedJobSerializer.fromEntity(entity, true);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private void changeJobStatus(String jobId,  Status status) {
    log.info(jobId + ": Changing job status to " + status);
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobStateImpl<T> jobState = lookupJobState(tx, jobId);
      if (jobState == null || !jobState.getStatus().isActive()) {
        log.info(jobId + ": Job not active, can't change its status: " + jobState);
        return;
      }
      jobState.setStatus(status);
      DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(tx, jobState));
      tx.commit();
    } finally {
      rollbackIfActive(tx);
    }
  }

  private void rollbackIfActive(Transaction tx) {
    try {
      if (tx.isActive()) {
        tx.rollback();
      }
    } catch (RuntimeException e) {
      log.log(Level.WARNING, "Rollback of transaction failed: ", e);
    }
  }

  void abortJob(String jobId) {
    changeJobStatus(jobId, new Status(ABORTED));
  }

  boolean cleanupJob(String jobId) {
    ShardedJobStateImpl<T> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      return true;
    }
    if (jobState.getStatus().isActive()) {
      return false;
    }
    int taskCount = jobState.getTotalTaskCount();
    if (taskCount > 0) {
      PipelineService pipeline = PipelineServiceFactory.newPipelineService();
      pipeline.startNewPipeline(new DeleteShardedJob(jobId, taskCount));
    }
    final Key jobKey = ShardedJobStateImpl.ShardedJobSerializer.makeKey(jobId);
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override
      public void run() {
        DATASTORE.delete(jobKey);
      }
    }), DATASTORE_RETRY_FOREVER_PARAMS, EXCEPTION_HANDLER);
    return true;
  }
}
