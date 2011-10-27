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
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.tools.mapreduce.v2.impl.MapReduceState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;

import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

/**
 * AppEngineJobContext extends Hadoop's JobContext to make extracting
 * information relevant to running a job on AppEngine easier.
 *
 * <p>Essentially, this class handles all reading of configuration variables,
 * delegating appropriately to the configuration or the request state.
 *
 */
public class AppEngineJobContext extends JobContext {
  // TODO(user): Make these private after we figure out our equivalent of JobConf.

  // Key names for values serialized in the configuration

  /**
   * The {@code Configuration} key for the entry naming the task queue to
   * enqueue controller tasks in.
   */
  public static final String CONTROLLER_QUEUE_KEY = "mapreduce.appengine.controller.queue";

  /**
   * The {@code Configuration} key for the entry naming the task queue to
   * enqueue worker tasks in.
   */
  public static final String WORKER_QUEUE_KEY = "mapreduce.appengine.mapper.queue";

  /**
   * The {@code Configuration} key for the entry naming the task queue to
   * enqueue the done callback in.
   */
  public static final String DONE_CALLBACK_QUEUE_KEY = "mapreduce.appengine.donecallback.queue";


  /**
   * The {@code Configuration} key for the entry denoting the maximum overall
   * rate of map() calls/second.
   */
  public static final String MAPPER_INPUT_PROCESSING_RATE_KEY
      = "mapreduce.mapper.inputprocessingrate";

  /**
   * The {@code Configuration} key for the entry denoting the number of parallel
   * mapper worker shards to start in parallel.
   */
  public static final String MAPPER_SHARD_COUNT_KEY = "mapreduce.mapper.shardcount";

  /**
   * The {@code Configuration} key for the entry containing the URL to be given
   * to the task queue for the done callback.
   */
  public static final String DONE_CALLBACK_URL_KEY = "mapreduce.appengine.donecallback.url";

  // Parameter names for values serialized in the request
  // All VisibleForTesting
  public static final String CONFIGURATION_PARAMETER_NAME = "configuration";
  public static final String JOB_ID_PARAMETER_NAME = "jobID";
  public static final String SLICE_NUMBER_PARAMETER_NAME = "sliceNumber";

  /**
   * Default rate of map() calls
   */
  public static final int DEFAULT_MAP_INPUT_PROCESSING_RATE = 1000;

  /**
   * Default number of mappers to run in parallel.
   */
  public static final int DEFAULT_MAPPER_SHARD_COUNT = 8;
  public static final String QUEUE_NAME_HEADER = "X-AppEngine-QueueName";

  private final String queueName;
  private final int sliceNumber;

  /**
   * Initializes a JobContext from a request.
   *
   * @param request the request to initialize from
   *
   */
  public AppEngineJobContext(HttpServletRequest request) {
    this(getConfigurationFromRequest(request, false), getJobIDFromRequest(request), request);
  }

  private AppEngineJobContext(Configuration configuration, JobID jobId,
      HttpServletRequest request) {
    this(configuration, jobId,
        request.getHeader(QUEUE_NAME_HEADER),
        Integer.parseInt(request.getParameter(SLICE_NUMBER_PARAMETER_NAME)));
  }

  private AppEngineJobContext(Configuration configuration, JobID jobId, String queueName,
      int sliceNumber) {
    super(configuration, jobId);
    this.queueName = queueName != null ? queueName : "default";
    this.sliceNumber = sliceNumber;
  }

  /**
   * Create context for new mapreduce job.
   */
  public static AppEngineJobContext createContextForNewJob(Configuration configuration) {
    return new AppEngineJobContext(configuration, generateNewJobID(), null, 0);
  }

  // VisibleForTesting
  // TODO(user): kill this method
  public static AppEngineJobContext createContextForTesting(Configuration configuration,
      JobID jobId, HttpServletRequest request) {
    return new AppEngineJobContext(configuration, jobId, request);
  }

  /**
   * Gets the Configuration that was passed to this request.
   * The request must have a {@link #CONFIGURATION_PARAMETER_NAME} parameter.
   *
   * @param req the request currently being processed
   * @param startRequest whether or not this request is for the start handler
   * @return the corresponding configuration
   */
  protected static Configuration getConfigurationFromRequest(HttpServletRequest req,
      boolean startRequest) {
    String serializedConf;
    if (startRequest) {
      serializedConf = req.getParameter(CONFIGURATION_PARAMETER_NAME);
    } else {
      JobID jobId = getJobIDFromRequest(req);
      try {
        MapReduceState state = MapReduceState.getMapReduceStateFromJobID(
            DatastoreServiceFactory.getDatastoreService(),
            jobId);
        serializedConf = state.getConfigurationXML();
      } catch (EntityNotFoundException e) {
        // Likewise, this should have a real exception class but it's temporary.
        throw new RuntimeException("Couldn't find MR with job ID: " + jobId);
      }
    }

    return ConfigurationXmlUtil.getConfigurationFromXml(serializedConf);
  }

  /**
   * Generates a new unique Hadoop job ID.
   *
   * There's a whole idiom for how JobID is constructed.
   * See
   * <a href="http://hadoop.apache.org/common/docs/r0.20.0/api/org/apache/hadoop/mapreduce/TaskAttemptID.html">
   * TaskAttemptID</a> and the linked classes for details.
   *
   * In the interest of making everyone happy, we pretend like we're the world's
   * worst job tracker. It restarts each time we start a new MR. On the plus
   * side, every job is job #1!
   */
  protected static JobID generateNewJobID() {
    return new JobID(
        ("" + System.currentTimeMillis() + UUID.randomUUID()).replace("-", ""), 1);
  }


  /**
   * Gets the Job ID from the given request.
   *
   * @param req a servlet request with the job ID stored in the
   * {@link #JOB_ID_PARAMETER_NAME} parameter
   * @return the job ID
   */
  // VisibleForTesting
  static JobID getJobIDFromRequest(HttpServletRequest req) {
    String jobIdString = req.getParameter(JOB_ID_PARAMETER_NAME);
    if (jobIdString == null) {
      throw new RuntimeException("Couldn't get Job ID for request. Aborting!");
    }
    return JobID.forName(jobIdString);
  }

  /**
   * Given a {@code queueKey} that may exist in this context's
   * {@link Configuration}, attempts to retrieve the queue name corresponding to
   * the key, falling back on reasonable defaults otherwise.
   */
  // VisibleForTesting
  String getQueueName(String queueKey) {
    return getConfiguration().get(queueKey, queueName);
  }

  /**
   * Gets the taskqueue to enqueue worker tasks in.
   *
   * @return the worker taskqueue
   */
  public Queue getWorkerQueue() {
    return QueueFactory.getQueue(getQueueName(WORKER_QUEUE_KEY));
  }

  /**
   * Gets the task queue to enqueue controller tasks in.
   *
   * @return the controller taskqueue
   */
  public Queue getControllerQueue() {
    return QueueFactory.getQueue(getQueueName(CONTROLLER_QUEUE_KEY));
  }

  /**
   * Gets the task queue to enqueue the done callback task in.
   *
   * @return the done callback taskqueue
   */
  public Queue getDoneCallbackQueue() {
    return QueueFactory.getQueue(getQueueName(DONE_CALLBACK_QUEUE_KEY));
  }

  /**
   * Returns the input processing rate: this is the number of map() calls
   * that can be made per second.
   */
  public int getInputProcessingRate() {
    return getConfiguration().getInt(
        MAPPER_INPUT_PROCESSING_RATE_KEY, DEFAULT_MAP_INPUT_PROCESSING_RATE);
  }

  /**
   * Returns true if this job has a done callback registered in the configuration.
   */
  public boolean hasDoneCallback() {
    return getDoneCallbackUrl() != null;
  }

  /**
   * Returns the done callback url
   */
  public String getDoneCallbackUrl() {
    return getConfiguration().get(DONE_CALLBACK_URL_KEY);
  }

  /**
   * Returns the shard count: this is the number of parallel worker task
   * queue chains running at a time.
   */
  public int getMapperShardCount() {
    return getConfiguration().getInt(MAPPER_SHARD_COUNT_KEY, DEFAULT_MAPPER_SHARD_COUNT);
  }

  /**
   * Returns the slice number of this task queue execution. This is a
   * counter that is increased with each sequential execution in a particular
   * task queue chain.
   */
  public int getSliceNumber() {
    return sliceNumber;
  }
}
