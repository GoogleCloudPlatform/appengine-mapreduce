package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Query;

import java.util.Iterator;
import java.util.Map;


/**
 * Tests the format in which ShardedJobs are written to the datastore.
 *
 */
public class ShardedJobStorageTest extends EndToEndTestCase {

  private static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();

  public void testRoundTripJob() throws EntityNotFoundException {
    ShardedJobStateImpl<TestTask, Integer> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(job);
    DATASTORE.put(entity);
    Entity readEntity = DATASTORE.get(entity.getKey());
    assertEquals(entity, readEntity);
    ShardedJobStateImpl<TestTask, Integer> fromEntity =
        ShardedJobStateImpl.ShardedJobSerializer.fromEntity(readEntity);
    assertEquals(job.getJobId(), fromEntity.getJobId());
    assertEquals(job.getActiveTaskCount(), fromEntity.getActiveTaskCount());
    assertEquals(job.getMostRecentUpdateTimeMillis(), fromEntity.getMostRecentUpdateTimeMillis());
    assertEquals(job.getStartTimeMillis(), fromEntity.getStartTimeMillis());
    assertEquals(job.getTotalTaskCount(), fromEntity.getTotalTaskCount());
    assertEquals(job.getAggregateResult(), fromEntity.getAggregateResult());
    assertEquals(job.getSettings().toString(), fromEntity.getSettings().toString());
    assertEquals(job.getStatus(), fromEntity.getStatus());
    assertEquals(job.getController(), fromEntity.getController());
  }

  public void testExpectedFields() {
    ShardedJobStateImpl<TestTask, Integer> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(job);
    Map<String, Object> properties = entity.getProperties();
    assertEquals(10, properties.get("activeTaskCount"));
    assertEquals(10, properties.get("taskCount"));
    assertEquals(0L, properties.get("nextSequenceNumber"));
    assertTrue(properties.containsKey("status"));
    assertTrue(properties.containsKey("startTimeMillis"));
    assertTrue(properties.containsKey("settings"));
    assertTrue(properties.containsKey("mostRecentUpdateTimeMillis"));
  }


  public void testFetchJobById() throws EntityNotFoundException {
    ShardedJobStateImpl<TestTask, Integer> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(job);
    DATASTORE.put(entity);
    Entity readEntity = DATASTORE.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey("jobId"));
    assertEquals(entity, readEntity);
  }

  private ShardedJobStateImpl<TestTask, Integer> createGenericJobState() {
    return new ShardedJobStateImpl<TestTask, Integer>("jobId",
        new TestController(11),
        new ShardedJobSettings(),
        10,
        System.currentTimeMillis(),
        new Status(Status.StatusCode.INITIALIZING),
        1);
  }

  public void testQueryByKind() {
    Query query = new Query(ShardedJobStateImpl.ShardedJobSerializer.ENTITY_KIND);
    Iterator<Entity> iterable = DATASTORE.prepare(query).asIterable().iterator();
    assertEquals(false, iterable.hasNext());

    ShardedJobStateImpl<TestTask, Integer> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(job);
    DATASTORE.put(entity);

    Entity singleEntity = DATASTORE.prepare(query).asSingleEntity();
    assertEquals(entity, singleEntity);
  }

}
