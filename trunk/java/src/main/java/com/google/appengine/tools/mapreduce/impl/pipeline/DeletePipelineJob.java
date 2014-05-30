package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.api.taskqueue.DeferredTask;
import com.google.appengine.api.taskqueue.DeferredTaskContext;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.RetryOptions;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.pipeline.Job0;
import com.google.appengine.tools.pipeline.NoSuchObjectException;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.base.Optional;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;

/**
 * A Job to delete all records of a given pipeline.
 * When the job runs it creates a {@link DeferredTask} that is
 * responsible for deleting the pipeline. This enable to use
 * any rootJobKey including a parent of this job.
 */
public class DeletePipelineJob extends Job0<Void> {

  private static final long serialVersionUID = -5440838671291502355L;
  private static final Logger log = Logger.getLogger(DeletePipelineJob.class.getName());
  private final String key;

  public DeletePipelineJob(String rootJobKey) {
    this.key = rootJobKey;
  }

  @Override
  public Value<Void> run() {
    DeferredTask deleteRecordsTask = new DeferredTask() {
      private static final long serialVersionUID = -7510918963650055768L;

      @Override
      public void run() {
        PipelineService service = PipelineServiceFactory.newPipelineService();
        try {
          service.deletePipelineRecords(key);
          log.info("Deleted pipeline: " + key);
        } catch (IllegalStateException e) {
          log.warning("Failed to delete pipeline: " + key);
          HttpServletRequest request = DeferredTaskContext.getCurrentRequest();
          if (request != null) {
            int attempts = request.getIntHeader("X-AppEngine-TaskExecutionCount");
            if (attempts <= 5) {
              log.info("Request to retry deferred task #" + attempts);
              DeferredTaskContext.markForRetry();
              return;
            }
          }
          try {
            service.deletePipelineRecords(key, true, false);
            log.warning("Force deleted pipeline: " + key);
          } catch (Exception ex) {
            log.log(Level.WARNING, "Failed to force delete pipeline: " + key, ex);
          }
        } catch (NoSuchObjectException e) {
          // Already done
        }
      }
    };
    String queueName = Optional.fromNullable(getOnQueue()).or("default");
    Queue queue = QueueFactory.getQueue(queueName);
    queue.add(TaskOptions.Builder.withPayload(deleteRecordsTask).countdownMillis(10000)
        .retryOptions(RetryOptions.Builder.withMinBackoffSeconds(2).maxBackoffSeconds(20)));
    return null;
  }
}