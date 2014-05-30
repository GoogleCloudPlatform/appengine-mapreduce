package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapJob;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceJobException;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.DatastoreOutput;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;

import java.util.List;
import java.util.logging.Logger;

// [START chain_job_example]
/**
 * Runs three MapReduces in a row. The first creates random MapReduceTest entities of the type
 * {@link #datastoreType} The second counts the number of each character in all entities of this
 * type. The third deletes all entities of the {@link #datastoreType}
 */
public class ChainedMapReduceJob extends Job0<MapReduceResult<List<List<KeyValue<String, Long>>>>> {

  private static final long serialVersionUID = 6725038763886885189L;
  private static final Logger log = Logger.getLogger(ChainedMapReduceJob.class.getName());

  private final String bucket;
  private final String datastoreType;
  private final int shardCount;
  private final int entities;
  private final int bytesPerEntity;

  public ChainedMapReduceJob(String bucket, String datastoreType, int shardCount, int entities,
      int bytesPerEntity) {
    this.bucket = bucket;
    this.datastoreType = datastoreType;
    this.shardCount = shardCount;
    this.entities = entities;
    this.bytesPerEntity = bytesPerEntity;
  }

  @Override
  public FutureValue<MapReduceResult<List<List<KeyValue<String, Long>>>>> run() throws Exception {
    MapJob<Long, Entity, Void> createJob = new MapJob<>();
    MapReduceJob<Entity, String, Long, KeyValue<String, Long>, List<List<KeyValue<String, Long>>>>
        countJob = new MapReduceJob<>();
    MapJob<Entity, Void, Void> deleteJob = new MapJob<>();

    MapReduceSettings settings = getSettings(bucket);

    FutureValue<MapReduceResult<Void>> createFuture = futureCall(createJob,
        immediate(getCreationJobSpec(bytesPerEntity, entities, shardCount)), immediate(settings));

    FutureValue<MapReduceResult<List<List<KeyValue<String, Long>>>>> countFuture = futureCall(
        countJob, immediate(getCountJobSpec(shardCount, shardCount)), immediate(settings),
        waitFor(createFuture));

    futureCall(deleteJob, immediate(getDeleteJobSpec(shardCount)), immediate(settings),
        waitFor(countFuture));

    return countFuture;
  }

  public FutureValue<MapReduceResult<List<List<KeyValue<String, Long>>>>> handleException(
      MapReduceJobException exception) throws Throwable {
    // one of the child MapReduceJob has failed
    log.severe("MapReduce job failed because of: " + exception.getMessage());
    // ... Send an email, try again, ... or fail
    throw exception;
  }

  // ...
  // [END chain_job_example]

  MapReduceSettings getSettings(String bucket) {
    return new MapReduceSettings.Builder().setWorkerQueueName("mapreduce-workers")
        .setBucketName(bucket).setModule("mapreduce").build();
  }

  MapSpecification<Long, Entity, Void> getCreationJobSpec(int bytesPerEntity, int entities,
      int shardCount) {
    return new MapSpecification.Builder<>(new ConsecutiveLongInput(0, entities, shardCount),
        new EntityCreator(datastoreType, bytesPerEntity), new DatastoreOutput()).setJobName(
        "Create MapReduce entities").build();
  }

  MapReduceSpecification<Entity, String, Long, KeyValue<String, Long>,
      List<List<KeyValue<String, Long>>>> getCountJobSpec(int mapShardCount, int reduceShardCount) {
    return new MapReduceSpecification.Builder<>(new DatastoreInput(datastoreType, mapShardCount),
        new CountMapper(), new CountReducer(), new InMemoryOutput<KeyValue<String, Long>>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setJobName("MapReduceTest count")
        .setNumReducers(reduceShardCount)
        .build();
  }

  MapSpecification<Entity, Void, Void> getDeleteJobSpec(int mapShardCount) {
    return new MapSpecification.Builder<Entity, Void, Void>(
        new DatastoreInput(datastoreType, mapShardCount), new DeleteEntityMapper(null)).setJobName(
        "Delete MapReduce entities").build();
  }
}
