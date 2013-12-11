// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ABORTED;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ERROR;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;
import static java.util.concurrent.Executors.callable;

import com.google.appengine.api.backends.BackendServiceFactory;
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
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.apphosting.api.ApiProxy.ApiProxyException;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Contains all logic to manage and run sharded jobs.
 *
 * This is a helper class for {@link ShardedJobServiceImpl} that implements
 * all the functionality but assumes fixed types for {@code <T>} and
 * {@code <R>}.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <T> type of tasks that the job being processed consists of
 * @param <R> type of intermediate and final results of the job being processed
 */
class ShardedJobRunner<T extends IncrementalTask<T, R>, R extends Serializable> {

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

  static final String JOB_ID_PARAM = "job";
  static final String TASK_ID_PARAM = "task";
  static final String SEQUENCE_NUMBER_PARAM = "seq";

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
      DatastoreTimeoutException.class).build();

  private ShardedJobStateImpl<T, R> lookupJobState(Transaction tx, String jobId) {
    try {
      Entity entity = DATASTORE.get(tx, ShardedJobStateImpl.ShardedJobSerializer.makeKey(jobId));
      return ShardedJobStateImpl.ShardedJobSerializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private IncrementalTaskState<T, R> lookupTaskState(Transaction tx, String taskId) {
    try {
      Entity entity = DATASTORE.get(tx, IncrementalTaskState.Serializer.makeKey(taskId));
      return IncrementalTaskState.Serializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private ShardRetryState<T, R> lookupShardRetryState(String taskId) {
    try {
      Entity entity = DATASTORE.get(ShardRetryState.Serializer.makeKey(taskId));
      return ShardRetryState.Serializer.fromEntity(entity);
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private static Iterator<Entity> lookupTasks(final String jobId, final int taskCount) {
    return new AbstractIterator<Entity>() {
      private int lastCount;
      private Iterator<Map.Entry<Key, Entity>> lastBatch = Iterators.emptyIterator();

      @Override protected Entity computeNext() {
        if (lastBatch.hasNext()) {
          Map.Entry<Key, Entity> entry = lastBatch.next();
          Entity entity = entry.getValue();
          Preconditions.checkState(entity != null, "%s: Missing task: %s", jobId, entry.getKey());
          return entity;
        } else if (lastCount >= taskCount) {
          return endOfData();
        }
        int toRead = Math.min(20, taskCount - lastCount);
        List<Key> keys = new ArrayList<>(toRead);
        for (int i = 0; i < toRead; i++, lastCount++) {
          Key key = IncrementalTaskState.Serializer.makeKey(getTaskId(jobId, lastCount));
          keys.add(key);
        }
        lastBatch = DATASTORE.get(keys).entrySet().iterator();
        return computeNext();
      }
    };
  }

  private R aggregateState(ShardedJobController<T, R> controller, ShardedJobState<?, ?> jobState) {
    ImmutableList.Builder<R> results = ImmutableList.builder();
    for (Iterator<Entity> iter = lookupTasks(jobState.getJobId(), jobState.getTotalTaskCount());
        iter.hasNext();) {
      Entity entity = iter.next();
      IncrementalTaskState<T, R> state = IncrementalTaskState.Serializer.<T, R>fromEntity(entity);
      results.add(state.getPartialResult());
    }
    return controller.combineResults(results.build());
  }

  private void scheduleControllerTask(Transaction tx, String jobId, String taskId,
      ShardedJobSettings settings) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getControllerPath()).param(JOB_ID_PARAM, jobId).param(TASK_ID_PARAM, taskId);
    if (settings.getControllerBackend() != null) {
      taskOptions.header("Host", BackendServiceFactory.getBackendService().getBackendAddress(
          settings.getControllerBackend()));
    }
    QueueFactory.getQueue(settings.getControllerQueueName()).add(tx, taskOptions);
  }

  private void scheduleWorkerTask(Transaction tx,
      ShardedJobSettings settings, IncrementalTaskState<T, R> state) {
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getWorkerPath())
        .param(TASK_ID_PARAM, state.getTaskId())
        .param(JOB_ID_PARAM, state.getJobId())
        .param(SEQUENCE_NUMBER_PARAM, String.valueOf(state.getNextSequenceNumber()));
    if (settings.getWorkerBackend() != null) {
      taskOptions.header("Host",
          BackendServiceFactory.getBackendService().getBackendAddress(settings.getWorkerBackend()));
    }
    QueueFactory.getQueue(settings.getWorkerQueueName()).add(tx, taskOptions);
  }

  void completeShard(final String jobId, final String taskId) {
    log.info("Polling task states for job " + jobId);
    final int shardNumber = parseTaskNumberFromTaskId(jobId, taskId);
    ShardedJobState<T, R> jobState =
        RetryHelper.runWithRetries(new Callable<ShardedJobState<T, R>>() {
          @Override
          public ShardedJobState<T, R> call() throws ConcurrentModificationException,
              DatastoreFailureException {
            Transaction tx = DATASTORE.beginTransaction();
            try {
              ShardedJobStateImpl<T, R> jobState = lookupJobState(tx, jobId);
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
              if (tx.isActive()) {
                tx.rollback();
              }
            }
          }
        }, DATASTORE_RETRY_PARAMS , EXCEPTION_HANDLER);
    if (jobState.getActiveTaskCount() == 0) {
      log.info("Calling completed on: " + jobId);
      if (jobState.getStatus().getStatusCode() == DONE) {
        jobState.getController().completed(aggregateState(jobState.getController(), jobState));
      } else {
        jobState.getController().failed(jobState.getStatus());
      }
    }
  }

  void runTask(final String taskId, final String jobId, final int sequenceNumber) {
    final ShardedJobState<T, R> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      log.info(taskId + ": Job gone");
      return;
    }
    final IncrementalTaskState<T, R> taskState = lookupTaskState(null, taskId);
    if (taskState == null) {
      log.info(taskId + ": Task gone");
      return;
    }
    String statusUrl = jobState.getSettings().getPipelineStatusUrl();
    log.info("Running task " + taskId + " (job " + jobId + "), sequence number " + sequenceNumber
        + (statusUrl != null ? " Progress can be monitored at: " + statusUrl : ""));

    if (taskState.getNextSequenceNumber() != sequenceNumber) {
      Preconditions.checkState(taskState.getNextSequenceNumber() > sequenceNumber,
          "%s: Task state is from the past: %s", taskId, taskState);
      log.info(taskId + ": Task sequence number " + sequenceNumber
          + " already completed: " + taskState);
      return;
    }
    Preconditions.checkState(taskState.getNextTask() != null, "%s: Next task is null", taskState);

    ShardRetryState<T, R> retryState = null;
    if (!jobState.getStatus().isActive()) {
      log.info(taskId + ": Job no longer active: " + jobState);
      taskState.setNextTask(null);
    } else {
      log.fine("About to run task: " + taskState);
      try {
        IncrementalTask.RunResult<T, R> result = taskState.getNextTask().run();
        taskState.setPartialResult(
            jobState.getController().combineResults(
                ImmutableList.of(taskState.getPartialResult(), result.getPartialResult())));
        taskState.setNextTask(result.getFollowupTask());
        taskState.clearRetryCount();
      } catch (RejectRequestException ex) {
        // No work should have been done.
        throw ex;
      } catch (ShardFailureException  ex) {
        retryState = handleShardFailure(jobState, taskState, ex);
      } catch (RuntimeException ex) {
        retryState = handleSliceFailure(jobState, taskState, ex);
      } catch (Error ex) {
        log.log(Level.WARNING, "Slice encountered an Error.");
        retryState = handleShardFailure(jobState, taskState, new RuntimeException("Error", ex));
      }
    }

    final ShardRetryState<T, R> shardRetryState = retryState;
    try {
      RetryHelper.runWithRetries(callable(new Runnable() {
        @Override
        public void run() {
          updateTask(jobState, taskState, shardRetryState);
        }
      }), DATASTORE_RETRY_FOREVER_PARAMS , EXCEPTION_HANDLER);
    } catch (RetryHelperException ex) {
      log.severe("Failed to write end of slice. Serialzing next task: "
          + taskState.getNextTask() + " Result: " + taskState.getPartialResult());
      // TODO(user): consider what to do here when this fail (though options are limited)
      throw ex;
    }
  }

  private ShardRetryState<T, R> handleSliceFailure(ShardedJobState<T, R> jobState,
      IncrementalTaskState<T, R> taskState, RuntimeException ex) {
    int attempts = taskState.incrementAndGetRetryCount();
    if (attempts > jobState.getSettings().getMaxSliceRetries()){
      log.log(Level.WARNING, "Slice exceeded its max attempts.");
      return handleShardFailure(jobState, taskState, ex);
    } else {
      log.log(Level.WARNING, "Slice attempt #" + attempts + " failed. Going to retry.", ex);
    }
    return null;
  }

  private void updateTask(ShardedJobState<T, R> jobState,
      IncrementalTaskState<T, R> taskState, /*Nullable*/ ShardRetryState<T, R> shardRetryState) {
    String taskId = taskState.getTaskId();
    int existingSequenceNumber = taskState.getNextSequenceNumber();
    taskState.setNextSequenceNumber(existingSequenceNumber + 1);
    taskState.setMostRecentUpdateMillis(System.currentTimeMillis());

    Transaction tx = DATASTORE.beginTransaction();
    try {
      IncrementalTaskState<T, R> existing = lookupTaskState(tx, taskId);
      if (existing == null) {
        log.info(taskId + ": Ignoring an update, as task disappeared while processing");
      } else if (existing.getNextSequenceNumber() != existingSequenceNumber) {
        log.info(taskId + ": Ignoreing an update, Task processed concurrently;" +
            " was sequence number " + existingSequenceNumber + ", now " +
            existing.getNextSequenceNumber());
      } else {
        if (taskState.getRetryCount() > existing.getRetryCount()) {
          // Slice retry, we need to reset state and partial result
          taskState.setNextTask(existing.getNextTask());
          taskState.setPartialResult(existing.getPartialResult());
        }
        Entity taskStateEntity = IncrementalTaskState.Serializer.toEntity(taskState);
        if (shardRetryState == null) {
          DATASTORE.put(tx, taskStateEntity);
        } else {
          Entity retryStateEntity = ShardRetryState.Serializer.toEntity(shardRetryState);
          DATASTORE.put(tx, Arrays.asList(taskStateEntity, retryStateEntity));
        }
        if (taskState.getNextTask() != null) {
          scheduleWorkerTask(tx, jobState.getSettings(), taskState);
        } else {
          scheduleControllerTask(tx, jobState.getJobId(), taskId, jobState.getSettings());
        }
        tx.commit();
      }
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  private ShardRetryState<T, R> handleShardFailure(final ShardedJobState<T, R> jobState,
      final IncrementalTaskState<T, R> taskState, Exception ex) {
    ShardRetryState<T, R> retryState = lookupShardRetryState(taskState.getTaskId());
    int attempts;
    if (retryState == null ||
        (attempts = retryState.incrementAndGet()) > jobState.getSettings().getMaxShardRetries()) {
        log.log(Level.SEVERE,
            "Shard exceeded its max attempts, setting job state to ERROR.", ex);
        changeJobStatus(jobState.getJobId(), new Status(ERROR, ex));
        taskState.setNextTask(null);
    } else {
      log.log(Level.WARNING, "Shard attempt #" + attempts + " failed. Going to retry.", ex);
      taskState.setNextTask(retryState.getInitialTask());
      taskState.clearRetryCount();
      Iterable<R> emptyResult = ImmutableList.<R>of();
      taskState.setPartialResult(jobState.getController().combineResults(emptyResult));
    }
    return retryState;
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

  private void createTasks(ShardedJobController<T, R> controller, ShardedJobSettings settings,
      String jobId, List<? extends T> initialTasks, long startTimeMillis) {
    log.info(jobId + ": Creating " + initialTasks.size() + " tasks");
    R initialResult = controller.combineResults(ImmutableList.<R>of());
    int id = 0;
    for (T initialTask : initialTasks) {
      // TODO(user): shardId (as known to WorkerShardTask) and taskId happen to be the same
      // number, just because they are created in the same order and happen to use their ordinal.
      // We should have way to inject the "shard-id" to the task.
      String taskId = getTaskId(jobId, id++);
      Transaction tx = DATASTORE.beginTransaction();
      try {
        IncrementalTaskState<T, R> taskState = lookupTaskState(tx, taskId);
        if (taskState != null) {
          log.info(jobId + ": Task already exists: " + taskState);
          continue;
        }
        taskState = IncrementalTaskState.<T, R>create(
            taskId, jobId, startTimeMillis, initialTask, initialResult);
        ShardRetryState<T, R> retryState = ShardRetryState.createFor(taskState);
        DATASTORE.put(tx, Arrays.asList(IncrementalTaskState.Serializer.toEntity(taskState),
            ShardRetryState.Serializer.toEntity(retryState)));
        scheduleWorkerTask(tx, settings, taskState);
        tx.commit();
      } finally {
        if (tx.isActive()) {
          tx.rollback();
        }
      }
    }
  }

  private void writeInitialJobState(ShardedJobStateImpl<T, R> jobState) {
    String jobId = jobState.getJobId();
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobState<T, R> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
        tx.commit();
        log.info(jobId + ": Writing initial job state");
      } else {
        log.info(jobId + ": Ignoring Attempt to reinitialize job state: " + existing);
      }
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  void startJob(final String jobId, List<? extends T> initialTasks,
      ShardedJobController<T, R> controller, ShardedJobSettings settings) {
    long startTimeMillis = System.currentTimeMillis();
    Preconditions.checkArgument(!Iterables.any(initialTasks, Predicates.isNull()),
        "Task list must not contain null values");
    ShardedJobStateImpl<T, R> jobState = new ShardedJobStateImpl<T, R>(jobId, controller, settings,
        initialTasks.size(), startTimeMillis, new Status(RUNNING));
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      jobState.setStatus(new Status(DONE));
      DATASTORE.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      controller.completed(controller.combineResults(ImmutableList.<R>of()));
      return;
    }
    writeInitialJobState(jobState);
    createTasks(controller, settings, jobId, initialTasks, startTimeMillis);
    log.info(jobId + ": All tasks were created");
  }

  ShardedJobState<T, R> getJobState(String jobId) {
    ShardedJobStateImpl<T, R> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      return null;
    }
    // We don't pre-aggregate the result across all tasks since it might not fit
    // in one entity.  The stored value is always null.
    Preconditions.checkState(jobState.getAggregateResult() == null,
        "%s: Non-null aggregate result: %s", jobState, jobState.getAggregateResult());
    R aggregateResult = aggregateState(jobState.getController(), jobState);
    jobState.setAggregateResult(aggregateResult);
    return jobState;
  }

  private void changeJobStatus(String jobId,  Status status) {
    log.info(jobId + ": Changing job status to " + status);
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobStateImpl<T, R> jobState = lookupJobState(tx, jobId);
      if (jobState == null || !jobState.getStatus().isActive()) {
        log.info(jobId + ": Job not active, can't change its status: " + jobState);
        return;
      }
      jobState.setStatus(status);
      DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      tx.commit();
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  void abortJob(String jobId) {
    changeJobStatus(jobId, new Status(ABORTED));
  }
}
