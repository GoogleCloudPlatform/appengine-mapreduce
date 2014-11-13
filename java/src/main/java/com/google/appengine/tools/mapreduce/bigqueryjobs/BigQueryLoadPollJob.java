package com.google.appengine.tools.mapreduce.bigqueryjobs;

import com.google.api.services.bigquery.model.Job;
import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.mapreduce.impl.BigQueryConstants;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.JobInfo.State;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.OrphanedObjectException;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;
import com.google.appengine.tools.pipeline.impl.PipelineManager;
import com.google.common.base.Optional;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 * A pipeline job to poll the status of a bigquery load {@link Job}. It polls until the job goes to
 * completion or it fails.
 */
final class BigQueryLoadPollJob extends Job1<Void, BigQueryLoadJobReference> {

  private static final long serialVersionUID = 3156995046607209969L;
  private static final Logger log = Logger.getLogger(BigQueryLoadPollJob.class.getName());
  private final String jobStatusHandle;

  /**
   * @param jobStatusHandle handle that is populated by this job on completion.
   */
  BigQueryLoadPollJob(String jobStatusHandle) {
    this.jobStatusHandle = jobStatusHandle;
  }

  @Override
  public Value<Void> run(final BigQueryLoadJobReference jobToPoll) throws Exception {
    DeferredTask pollTask = new DeferredTask() {
      private static final long serialVersionUID = 6194863971729831899L;

      @Override
      public void run() {
        String jobRef = jobToPoll.getJobReference().getJobId();
        try {
          Job pollJob = BigQueryLoadGoogleCloudStorageFilesJob.getBigquery().jobs()
              .get(jobToPoll.getJobReference().getProjectId(), jobRef).execute();
          log.info("Job status of job " + jobRef + " : " + pollJob.getStatus().getState());
          if (pollJob.getStatus().getState().equals("PENDING")
              || pollJob.getStatus().getState().equals("RUNNING")) {
            setForRetry();
          } else {
            submitPromisedValue(pollJob.getStatus().getState());
          }
        } catch (IOException e) {
          log.warning("Unable to poll the status of the job " + jobRef + " . Retrying after "
              + BigQueryConstants.MIN_TIME_BEFORE_NEXT_POLL + " seconds");
          setForRetry();
        }
      }

      private void setForRetry() {
        HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
        int attempts = request.getIntHeader("X-AppEngine-TaskExecutionCount");
        log.info("Request to poll the job status #" + attempts);
        DeferredTaskContext.markForRetry();
      }

      private void submitPromisedValue(String status) {
        try {
          PipelineServiceFactory.newPipelineService().submitPromisedValue(jobStatusHandle, status);
        } catch (OrphanedObjectException e) {
          log.warning("Discarding an orphaned promiseHandle: " + jobStatusHandle);
        } catch (NoSuchObjectException e) {
          try {
            if (State.RUNNING.equals(
                PipelineManager.getJob(getPipelineKey().getName()).getJobState())) {
              throw new RuntimeException(
                  "No handle found for the promise. Root job is still running. So task queue will retry. Pipeline url. " + getStatusConsoleUrl());
            }
          } catch (NoSuchObjectException e1) {
            // Pipeline is complete. Return.
          }
        }
      }
    };
    String queueName = Optional.fromNullable(getOnQueue()).or("default");
    Queue queue = QueueFactory.getQueue(queueName);
    queue.add(TaskOptions.Builder.withPayload(pollTask)
        .countdownMillis(10000).retryOptions(RetryOptions.Builder.withMinBackoffSeconds(
            BigQueryConstants.MIN_TIME_BEFORE_NEXT_POLL).maxBackoffSeconds(
            BigQueryConstants.MAX_TIME_BEFORE_NEXT_POLL)));
    return null;
  }
}
