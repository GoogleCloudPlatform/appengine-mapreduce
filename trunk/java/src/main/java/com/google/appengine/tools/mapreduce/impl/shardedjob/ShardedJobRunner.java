// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ABORTED;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ERROR;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;
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
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TransactionalTaskException;
import com.google.appengine.api.taskqueue.TransientFailureException;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.apphosting.api.ApiProxy.ArgumentException;
import com.google.apphosting.api.ApiProxy.RequestTooLargeException;
import com.google.apphosting.api.ApiProxy.ResponseTooLargeException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
  // A sharded job is started with a given number of tasks, and every task
  // has zero or one follow-up task; so the total number of tasks never
  // increases.  We assign each follow-up task the same taskId as its
  // predecessor and reuse the same datastore entity to store it.  So, each
  // taskId really represents an entire chain of tasks, and the set of such task
  // chains is known at startup.
  //
  // Each task chain is its own entity group to avoid contention.
  //
  // (We could extend the API later to allow more than one follow-up task, but
  // that either leads to datastore contention, or makes finding the set of all
  // task entities harder; so, since we don't need more than one follow-up task
  // for now, we use a single entity for each chain of follow-up tasks.)
  //
  // There is also a single entity (in its own entity group) that holds the
  // overall job state.  It is updated only during initialization and from the
  // controller.
  //
  // Partial results of each task and its chain of follow-up tasks are combined
  // incrementally as the tasks complete.  Partial results across chains are
  // combined only when the job completes, or when getJobState() is called.  (We
  // could have the controller store the overall combined result, but there's a
  // risk that it doesn't fit in a single entity, so we don't.
  // ShardedJobStateImpl has a field for it and the Serializer supports it
  // for completeness, but the value in the datastore is always null.)
  //
  // Worker and controller tasks and entities carry a strictly monotonic
  // "sequence number" that allows each task to detect if its work has already
  // been done (useful in case the task queue runs it twice).  We schedule each
  // task in the same datastore transaction that updates the sequence number in
  // the entity.
  //
  // Each task also checks the job state entity to detect if the job has been
  // aborted or deleted, and terminates if so.
  //
  // We make job startup idempotent by letting the caller specify the job id
  // (rather than generating one randomly), and deriving task ids from it in a
  // deterministic fashion.  This makes it possible to schedule sharded jobs
  // from Pipeline jobs with no danger of scheduling a duplicate sharded job if
  // Pipeline or the task queue runs a job twice.  (For example, a caller could
  // derive the job id for the sharded job from the Pipeline job id.)

  private static final Logger log = Logger.getLogger(ShardedJobRunner.class.getName());

  private static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();

  private static final RetryParams DATASTORE_RETRY_PARAMS = new RetryParams.Builder()
      .initialRetryDelayMillis(1000).maxRetryDelayMillis(30000).retryMinAttempts(5).build();

  private static final RetryParams DATASTORE_RETRY_FOREVER_PARAMS =
      new RetryParams.Builder(DATASTORE_RETRY_PARAMS)
          .retryMaxAttempts(Integer.MAX_VALUE)
          .totalRetryPeriodMillis(Long.MAX_VALUE)
          .build();

  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
      ApiProxyException.class, ConcurrentModificationException.class,
      DatastoreFailureException.class, CommittedButStillApplyingException.class,
      DatastoreTimeoutException.class, TransientFailureException.class,
      TransactionalTaskException.class).abortOn(RequestTooLargeException.class,
      ResponseTooLargeException.class, ArgumentException.class).build();

  private static final ExceptionHandler AGGRESIVE_EXCEPTION_HANDLER =
      new ExceptionHandler.Builder().retryOn(Exception.class).abortOn(
      IllegalArgumentException.class, RequestTooLargeException.class,
      ResponseTooLargeException.class, ArgumentException.class).build();

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

  Iterator<IncrementalTaskState<T>> lookupTasks(final String jobId, final int taskCount) {
    return new AbstractIterator<IncrementalTaskState<T>>() {
      private int lastCount;
      private Iterator<Entity> lastBatch = Collections.emptyIterator();

      @Override
      protected IncrementalTaskState<T> computeNext() {
        if (lastBatch.hasNext()) {
          Entity entity = lastBatch.next();
          return IncrementalTaskState.Serializer.<T>fromEntity(entity);
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

  private void callCompleted(ShardedJobState<T> jobState) {
    ImmutableList.Builder<T> results = ImmutableList.builder();
    for (Iterator<IncrementalTaskState<T>> iter =
        lookupTasks(jobState.getJobId(), jobState.getTotalTaskCount()); iter.hasNext();) {
      results.add(iter.next().getTask());
    }
    jobState.getController().completed(results.build());
  }

  private void scheduleControllerTask(Transaction tx, String jobId, String taskId,
      ShardedJobSettings settings) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getControllerPath()).param(JOB_ID_PARAM, jobId)
        .param(TASK_ID_PARAM, taskId);
    taskOptions.header("Host", settings.getTaskQueueTarget());
    QueueFactory.getQueue(settings.getQueueName()).add(tx, taskOptions);
  }

  private void scheduleWorkerTask(Transaction tx, ShardedJobSettings settings,
      IncrementalTaskState<T> state, Long eta) {
    TaskOptions taskOptions = TaskOptions.Builder
        .withMethod(TaskOptions.Method.POST)
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
    ShardedJobState<T> jobState = RetryHelper.runWithRetries(new Callable<ShardedJobState<T>>() {
      @Override
      public ShardedJobState<T> call() throws ConcurrentModificationException,
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
          DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
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
    }
  }

  private IncrementalTaskState<T> getAndValidateTaskState(Transaction tx, String taskId,
      int sequenceNumber, ShardedJobState<T> jobState) {
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
      if (taskState.getSliceStartTime() == null) {
        return taskState;
      } else {
        handleLockHeld(taskId, jobState, taskState);
      }
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

  private void handleLockHeld(String taskId, ShardedJobState<T> jobState,
      IncrementalTaskState<T> taskState) {
    long currentTime = System.currentTimeMillis();
    int sliceTimeoutMillis = jobState.getSettings().getSliceTimeoutMillis();
    if (taskState.getSliceStartTime() + sliceTimeoutMillis > currentTime) {
      scheduleWorkerTask(null, jobState.getSettings(), taskState,
          taskState.getSliceStartTime() + sliceTimeoutMillis);
      log.info("Lock for " + taskId + " is being held. Will retry after "
          + (taskState.getSliceStartTime() + sliceTimeoutMillis - currentTime));
    } else {
      ShardRetryState<T> retryState = handleShardFailure(jobState, taskState, new RuntimeException(
          "Lock for " + taskId + " expired on slice: " + taskState.getSequenceNumber()));
      updateTask(jobState, taskState, retryState, false);
    }
  }

  private boolean lockShard(Transaction tx, ShardedJobState<T> jobState,
      IncrementalTaskState<T> taskState) {
    boolean locked = false;
    taskState.setSliceStartTime(System.currentTimeMillis());
    Entity entity = IncrementalTaskState.Serializer.toEntity(taskState);
    try {
      DATASTORE.put(tx, entity);
      tx.commit();
      locked = true;
    } catch (ConcurrentModificationException ex) {
      // TODO(user): would be nice to have a test for it. b/12822091 can help with that.
      log.warning("Failed to acquire the lock, Will reschedule task for: " + taskState.getJobId()
          + " on slice " + taskState.getSequenceNumber());
      long eta = System.currentTimeMillis() + new Random().nextInt(5000) + 5000;
      scheduleWorkerTask(null, jobState.getSettings(), taskState, eta);
    }
    return locked;
  }

  @Override
  public void runTask(final String jobId, final String taskId, final int sequenceNumber) {
    final ShardedJobState<T> jobState = lookupJobState(null, jobId);
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
      final ShardedJobState<T> jobState, IncrementalTaskState<T> taskState) {
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
      retryState = handleSliceFailure(jobState, taskState, ex);
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

  private ShardRetryState<T> handleSliceFailure(ShardedJobState<T> jobState,
      IncrementalTaskState<T> taskState, RuntimeException ex) {
    if (!(ex instanceof RecoverableException) && !taskState.getTask().allowSliceRetry()) {
      return handleShardFailure(jobState, taskState, ex);
    }
    int attempts = taskState.incrementAndGetRetryCount();
    if (attempts > jobState.getSettings().getMaxSliceRetries()){
      log.log(Level.WARNING, "Slice exceeded its max attempts.");
      return handleShardFailure(jobState, taskState, ex);
    } else {
      log.log(Level.WARNING, "Slice attempt #" + attempts + " failed. Going to retry.", ex);
    }
    return null;
  }

  private ShardRetryState<T> handleShardFailure(ShardedJobState<T> jobState,
      IncrementalTaskState<T> taskState, Exception ex) {
    ShardRetryState<T> retryState = lookupShardRetryState(taskState.getTaskId());
    if (retryState == null
        || retryState.incrementAndGet() > jobState.getSettings().getMaxShardRetries()) {
      log.log(Level.SEVERE, "Shard exceeded its max attempts, setting job state to ERROR.", ex);
      handleJobFailure(taskState, ex);
    } else {
      log.log(Level.WARNING,
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

  private void updateTask(final ShardedJobState<T> jobState,
      final IncrementalTaskState<T> taskState, /* Nullable */
      final ShardRetryState<T> shardRetryState, boolean aggresiveRetry) {
    taskState.setSequenceNumber(taskState.getSequenceNumber() + 1);
    taskState.setSliceStartTime(null);
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
        Entity taskStateEntity = IncrementalTaskState.Serializer.toEntity(taskState);
        if (shardRetryState == null) {
          DATASTORE.put(tx, taskStateEntity);
        } else {
          Entity retryStateEntity = ShardRetryState.Serializer.toEntity(shardRetryState);
          DATASTORE.put(tx, Arrays.asList(taskStateEntity, retryStateEntity));
        }
      }

      private void scheduleTask(ShardedJobState<T> jobState, IncrementalTaskState<T> taskState,
          Transaction tx) {
        if (taskState.getStatus().isActive()) {
          scheduleWorkerTask(tx, jobState.getSettings(), taskState, null);
        } else {
          scheduleControllerTask(tx, jobState.getJobId(), taskState.getTaskId(),
              jobState.getSettings());
        }
      }
    }), DATASTORE_RETRY_FOREVER_PARAMS, exceptionHandler);
  }

  private static String getTaskId(String jobId, int taskNumber) {
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
        DATASTORE.put(tx, Arrays.asList(IncrementalTaskState.Serializer.toEntity(taskState),
            ShardRetryState.Serializer.toEntity(retryState)));
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
      ShardedJobState<T> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
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
    long startTimeMillis = System.currentTimeMillis();
    Preconditions.checkArgument(!Iterables.any(initialTasks, Predicates.isNull()),
        "Task list must not contain null values");
    ShardedJobStateImpl<T> jobState = new ShardedJobStateImpl<>(jobId, controller, settings,
        initialTasks.size(), startTimeMillis, new Status(RUNNING));
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      jobState.setStatus(new Status(DONE));
      DATASTORE.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      controller.completed(initialTasks);
    } else {
      writeInitialJobState(jobState);
      createTasks(settings, jobId, initialTasks, startTimeMillis);
      log.info(jobId + ": All tasks were created");
    }
  }

  ShardedJobState<T> getJobState(String jobId) {
    return lookupJobState(null, jobId);
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
      DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
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
    } else if (jobState.getStatus().isActive()) {
      return false;
    }
    int taskCount = jobState.getTotalTaskCount();
    final Collection<Key> toDelete = new ArrayList<>(1 + 2 * taskCount);
    toDelete.add(ShardedJobStateImpl.ShardedJobSerializer.makeKey(jobId));
    for (int i = 0; i < taskCount; i++) {
      String taskId = getTaskId(jobId, i);
      toDelete.add(IncrementalTaskState.Serializer.makeKey(taskId));
      toDelete.add(ShardRetryState.Serializer.makeKey(taskId));
    }
    RetryHelper.runWithRetries(callable(new Runnable() {
      @Override
      public void run() {
        DATASTORE.delete(toDelete);
      }
    }), DATASTORE_RETRY_FOREVER_PARAMS, EXCEPTION_HANDLER);
    return true;
  }
}
