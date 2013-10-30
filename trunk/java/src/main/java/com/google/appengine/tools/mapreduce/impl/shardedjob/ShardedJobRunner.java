// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ABORTED;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.DONE;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.ERROR;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.INITIALIZING;
import static com.google.appengine.tools.mapreduce.impl.shardedjob.Status.StatusCode.RUNNING;

import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Transaction;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private static Iterator<Entity> lookupTasks(ShardedJobState<?, ?> jobState) {
    final String jobId = jobState.getJobId();
    final int taskCount = jobState.getTotalTaskCount();
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

  private static int countActiveTasks(ShardedJobState<?, ?> jobState) {
    int count = 0;
    for (Iterator<Entity> iter = lookupTasks(jobState); iter.hasNext(); ) {
      Entity entity = iter.next();
      if (IncrementalTaskState.Serializer.hasNextTask(entity)) {
        count++;
      }
    }
    return count;
  }

  private R aggregateState(ShardedJobController<T, R> controller, ShardedJobState<?, ?> jobState) {
    ImmutableList.Builder<R> results = ImmutableList.builder();
    for (Iterator<Entity> iter = lookupTasks(jobState); iter.hasNext(); ) {
      Entity entity = iter.next();
      IncrementalTaskState<T, R> state = IncrementalTaskState.Serializer.<T, R>fromEntity(entity);
      results.add(state.getPartialResult());
    }
    return controller.combineResults(results.build());
  }

  private void scheduleControllerTask(Transaction tx, ShardedJobStateImpl<T, R> state) {
    ShardedJobSettings settings = state.getSettings();
    TaskOptions taskOptions = TaskOptions.Builder.withMethod(TaskOptions.Method.POST)
        .url(settings.getControllerPath())
        .param(JOB_ID_PARAM, state.getJobId())
        .param(SEQUENCE_NUMBER_PARAM, String.valueOf(state.getNextSequenceNumber()))
        .countdownMillis(settings.getMillisBetweenPolls());
    if (settings.getControllerBackend() != null) {
      taskOptions.header("Host",
          BackendServiceFactory.getBackendService().getBackendAddress(
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

  void pollTaskStates(String jobId, int sequenceNumber) {
    ShardedJobStateImpl<T, R> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      log.info(jobId + ": Job gone");
      return;
    }
    log.info("Polling task states for job " + jobId + ", sequence number " + sequenceNumber);
    Preconditions.checkState(jobState.getStatus().getStatusCode() != INITIALIZING,
        "Should be done initializing: %s", jobState);
    if (jobState.getNextSequenceNumber() != sequenceNumber) {
      Preconditions.checkState(jobState.getNextSequenceNumber() > sequenceNumber,
          "%s: Job state is from the past: %s", jobId, jobState);
      log.info(jobId + ": Poll sequence number " + sequenceNumber
          + " already completed: " + jobState);
      return;
    }

    long currentPollTimeMillis = System.currentTimeMillis();
    int activeTasks = countActiveTasks(jobState);
    jobState.setMostRecentUpdateTimeMillis(currentPollTimeMillis);
    jobState.setActiveTaskCount(activeTasks);

    if (activeTasks == 0) {
      R aggregationResult = null;
      if (jobState.getStatus().isActive()) {
        aggregationResult = aggregateState(jobState.getController(), jobState);
        jobState.setStatus(new Status(DONE));
      }
      Status status = jobState.getStatus();
      if (status.getStatusCode() == DONE) {
        jobState.getController().completed(aggregationResult);
      } else {
        jobState.getController().failed(status);
      }
    } else {
      jobState.setNextSequenceNumber(sequenceNumber + 1);
    }

    log.fine(jobId + ": Writing " + jobState);
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobStateImpl<T, R> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        log.info(jobId + ": Job gone after poll");
        return;
      }
      if (existing.getNextSequenceNumber() != sequenceNumber) {
        log.info(jobId + ": Job processed concurrently; was sequence number " + sequenceNumber
            + ", now " + existing.getNextSequenceNumber());
        return;
      }
      DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      if (jobState.getStatus().isActive()) {
        scheduleControllerTask(tx, jobState);
      }
      tx.commit();
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  void runTask(String taskId, String jobId, int sequenceNumber) {
    ShardedJobState<T, R> jobState = lookupJobState(null, jobId);
    if (jobState == null) {
      log.info(taskId + ": Job gone");
      return;
    }
    IncrementalTaskState<T, R> taskState = lookupTaskState(null, taskId);
    if (taskState == null) {
      log.info(taskId + ": Task gone");
      return;
    }
    log.info("Running task " + taskId + " (job " + jobId + "), sequence number " + sequenceNumber);
    if (taskState.getNextSequenceNumber() != sequenceNumber) {
      Preconditions.checkState(taskState.getNextSequenceNumber() > sequenceNumber,
          "%s: Task state is from the past: %s", taskId, taskState);
      log.info(taskId + ": Task sequence number " + sequenceNumber
          + " already completed: " + taskState);
      return;
    }
    Preconditions.checkState(taskState.getNextTask() != null, "%s: Next task is null",
        taskState);

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
      } catch (ShardFailureException  ex) {
        retryState = lookupShardRetryState(taskId);
        if (retryState != null) {
          int retryCount = retryState.incrementAndGet();
          if (retryCount > jobState.getSettings().getMaxShardRetries()) {
            log.log(Level.SEVERE,
                "Shard failed (" + retryCount + "). Setting job state to ERROR.", ex);
            changeJobStatus(jobId, new Status(ERROR, ex));
            taskState.setNextTask(null);
          } else {
            log.log(Level.WARNING, "Shard failed (" + retryCount + "). Going to retry.", ex);
            taskState.setNextTask(retryState.getInitialTask());
            Iterable<R> emptyResult = ImmutableList.<R>of();
            taskState.setPartialResult(jobState.getController().combineResults(emptyResult));
          }
        }
      }
    }

    taskState.setMostRecentUpdateMillis(System.currentTimeMillis());
    taskState.setNextSequenceNumber(sequenceNumber + 1);
    // TOOD: retries.  we should only have one writer, so should have no
    // concurrency exceptions, but we should guard against other RPC failures.
    Transaction tx = DATASTORE.beginTransaction();
    Entity taskStateEntity = null;
    try {
      if (!wasTaskStateChanged(taskId, sequenceNumber, tx)) {
        taskStateEntity = IncrementalTaskState.Serializer.toEntity(taskState);
        if (retryState == null) {
          DATASTORE.put(tx, taskStateEntity);
        } else {
          Entity retryStateEntity = ShardRetryState.Serializer.toEntity(retryState);
          DATASTORE.put(tx, Arrays.asList(taskStateEntity, retryStateEntity));
        }
        if (taskState.getNextTask() != null) {
          scheduleWorkerTask(tx, jobState.getSettings(), taskState);
        }
        tx.commit();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to write end of slice. Serialzing next task: "
          + taskState.getNextTask() + " Result: " + taskState.getPartialResult()
          + "Serializing entity: " + taskStateEntity, e);
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  private boolean wasTaskStateChanged(String taskId, int sequenceNumber, Transaction tx) {
    IncrementalTaskState<?, ?> existing = lookupTaskState(tx, taskId);
    if (existing == null) {
      log.info(taskId + ": Task disappeared while processing");
      return true;
    }
    if (existing.getNextSequenceNumber() != sequenceNumber) {
      log.info(taskId + ": Task processed concurrently; was sequence number " + sequenceNumber
          + ", now " + existing.getNextSequenceNumber());
      return true;
    }
    return false;
  }

  private static String getTaskId(String jobId, int taskNumber) {
    return jobId + "-task" + taskNumber;
  }

  private void createTasks(ShardedJobController<T, R> controller, ShardedJobSettings settings,
      String jobId, List<? extends T> initialTasks, long startTimeMillis) {
    log.info(jobId + ": Creating " + initialTasks.size() + " tasks");
    // It's tempting to try to do a large batch put and a large batch enqueue,
    // but I don't see a way to make the batch put idempotent; we don't want to
    // clobber entities that record progress that has already been made.
    for (int i = 0; i < initialTasks.size(); i++) {
      String taskId = getTaskId(jobId, i);
      Transaction tx = DATASTORE.beginTransaction();
      try {
        IncrementalTaskState<T, R> taskState = lookupTaskState(tx, taskId);
        if (taskState != null) {
          log.info(jobId + ": Task already exists: " + taskState);
          continue;
        }
        R initialResult = controller.combineResults(ImmutableList.<R>of());
        taskState = IncrementalTaskState.<T, R>create(
            taskId, jobId, startTimeMillis, initialTasks.get(i), initialResult);
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

  // Returns true on success, false if the job is already finished according to the datastore.
  private boolean writeInitialJobState(ShardedJobStateImpl<T, R> jobState) {
    String jobId = jobState.getJobId();
    log.fine(jobId + ": Writing initial job state");
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobState<T, R> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
        tx.commit();
      } else {
        if (!existing.getStatus().isActive()) {
          // Maybe a concurrent initialization finished before us, and the
          // job was very quick and has already completed as well.
          log.info(jobId + ": Attempt to reinitialize inactive job: " + existing);
          return false;
        }
        log.info(jobId + ": Reinitializing job: " + existing);
      }
      return true;
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
  }

  private void scheduleControllerAndMarkActive(ShardedJobStateImpl<T, R> jobState) {
    String jobId = jobState.getJobId();
    log.fine(jobId + ": Scheduling controller and marking active");
    Transaction tx = DATASTORE.beginTransaction();
    try {
      ShardedJobStateImpl<T, R> existing = lookupJobState(tx, jobId);
      if (existing == null) {
        // Someone cleaned it up while we were still initializing.
        log.warning(jobId + ": Job disappeared while initializing");
        return;
      }
      if (existing.getStatus().getStatusCode() != INITIALIZING) {
        // Maybe a concurrent initialization finished first, or someone
        // cancelled it while we were initializing.
        log.info(jobId + ": Job changed status while initializing: " + jobState);
        return;
      }
      DATASTORE.put(tx, ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      scheduleControllerTask(tx, jobState);
      tx.commit();
    } finally {
      if (tx.isActive()) {
        tx.rollback();
      }
    }
    log.info(jobId + ": Started");
  }

  void startJob(String jobId, List<? extends T> rawInitialTasks,
      ShardedJobController<T, R> controller, ShardedJobSettings settings) {
    long startTimeMillis = System.currentTimeMillis();
    // ImmutableList.copyOf() checks for null elements.
    List<? extends T> initialTasks = ImmutableList.copyOf(rawInitialTasks);
    ShardedJobStateImpl<T, R> jobState = new ShardedJobStateImpl<T, R>(jobId, controller, settings,
        initialTasks.size(), startTimeMillis, new Status(INITIALIZING), null);
    if (initialTasks.isEmpty()) {
      log.info(jobId + ": No tasks, immediately complete: " + controller);
      Status status = new Status(DONE);
      jobState.setStatus(status);
      DATASTORE.put(ShardedJobStateImpl.ShardedJobSerializer.toEntity(jobState));
      controller.completed(controller.combineResults(ImmutableList.<R>of()));
      return;
    }
    if (!writeInitialJobState(jobState)) {
      return;
    }
    createTasks(controller, settings, jobId, initialTasks, startTimeMillis);
    jobState.setStatus(new Status(RUNNING));
    scheduleControllerAndMarkActive(jobState);
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
