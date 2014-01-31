package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceJob;
import com.google.appengine.tools.mapreduce.MapReduceJobException;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.pipeline.FutureValue;
import com.google.appengine.tools.pipeline.Job0;

import java.util.List;
import java.util.logging.Logger;

// [START chain_job_example]
/**
 * Runs three MapReduces in a row.
 * The first creates random MapReduceTest entities of the type {@link #datastoreType}
 * The second counts the number of each character in all entities of this type.
 * The third deletes all entities of the {@link #datastoreType}
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
    MapReduceJob<Long, Void, Void, Void, Void> createJob = new MapReduceJob<>();
    MapReduceJob<Entity, String, Long, KeyValue<String, Long>, List<List<KeyValue<String, Long>>>>
        countJob = new MapReduceJob<>();
    MapReduceJob<Entity, Void, Void, Void, Void> deleteJob = new MapReduceJob<>();

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

  //...
  // [END chain_job_example]

  MapReduceSettings getSettings(String bucket) {
    return new MapReduceSettings().setWorkerQueueName("mapreduce-workers").setBucketName(bucket)
        .setModule("mapreduce");
  }

  MapReduceSpecification<Long, Void, Void, Void, Void> getCreationJobSpec(int bytesPerEntity,
      int entities, int shardCount) {
    return MapReduceSpecification.of("Create MapReduce entities",
        new ConsecutiveLongInput(0, entities, shardCount),
        new EntityCreator(datastoreType, bytesPerEntity),
        Marshallers.getVoidMarshaller(),
        Marshallers.getVoidMarshaller(),
        NoReducer.<Void, Void, Void>create(),
        NoOutput.<Void, Void>create(1));
  }

  MapReduceSpecification<Entity, String, Long, KeyValue<String, Long>,
      List<List<KeyValue<String, Long>>>> getCountJobSpec(int mapShardCount, int reduceShardCount) {
    return MapReduceSpecification.of("MapReduceTest count",
        new DatastoreInput(datastoreType, mapShardCount),
        new CountMapper(),
        Marshallers.getStringMarshaller(),
        Marshallers.getLongMarshaller(),
        new CountReducer(),
        new InMemoryOutput<KeyValue<String, Long>>(reduceShardCount));
  }

  MapReduceSpecification<Entity, Void, Void, Void, Void> getDeleteJobSpec(int mapShardCount) {
    return MapReduceSpecification.of("Delete MapReduce entities",
        new DatastoreInput(datastoreType, mapShardCount),
        new DeleteEntityMapper(null),
        Marshallers.getVoidMarshaller(),
        Marshallers.getVoidMarshaller(),
        NoReducer.<Void, Void, Void>create(),
        NoOutput.<Void, Void>create(1));
  }
}
