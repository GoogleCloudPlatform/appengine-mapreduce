package com.google.appengine.tools.mapreduce.impl.shardedjob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.EndToEndTestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Iterator;
import java.util.Map;

/**
 * Tests the format in which ShardedJobs are written to the datastore.
 *
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class ShardedJobStorageTest extends EndToEndTestCase {

  private static final DatastoreService DATASTORE = DatastoreServiceFactory.getDatastoreService();

  @Test
  public void testRoundTripJob() throws EntityNotFoundException {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    DATASTORE.put(entity);
    Entity readEntity = DATASTORE.get(entity.getKey());
    assertEquals(entity, readEntity);
    ShardedJobStateImpl<TestTask> fromEntity =
        ShardedJobStateImpl.ShardedJobSerializer.fromEntity(readEntity);
    assertEquals(job.getJobId(), fromEntity.getJobId());
    assertEquals(job.getActiveTaskCount(), fromEntity.getActiveTaskCount());
    assertEquals(job.getMostRecentUpdateTimeMillis(), fromEntity.getMostRecentUpdateTimeMillis());
    assertEquals(job.getStartTimeMillis(), fromEntity.getStartTimeMillis());
    assertEquals(job.getTotalTaskCount(), fromEntity.getTotalTaskCount());
    assertEquals(job.getSettings().toString(), fromEntity.getSettings().toString());
    assertEquals(job.getStatus(), fromEntity.getStatus());
    assertEquals(job.getController(), fromEntity.getController());
  }

  @Test
  public void testExpectedFields() {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    Map<String, Object> properties = entity.getProperties();
    assertEquals(10, properties.get("taskCount"));
    assertTrue(properties.containsKey("activeShards"));
    assertTrue(properties.containsKey("status"));
    assertTrue(properties.containsKey("startTimeMillis"));
    assertTrue(properties.containsKey("settings"));
    assertTrue(properties.containsKey("mostRecentUpdateTimeMillis"));
  }

  @Test
  public void testFetchJobById() throws EntityNotFoundException {
    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    DATASTORE.put(entity);
    Entity readEntity = DATASTORE.get(ShardedJobStateImpl.ShardedJobSerializer.makeKey("jobId"));
    assertEquals(entity, readEntity);
  }

  private ShardedJobStateImpl<TestTask> createGenericJobState() {
    return ShardedJobStateImpl.create("jobId", new TestController(11),
        new ShardedJobSettings.Builder().build(), 10, System.currentTimeMillis());
  }

  @Test
  public void testQueryByKind() {
    Query query = new Query(ShardedJobStateImpl.ShardedJobSerializer.ENTITY_KIND);
    Iterator<Entity> iterable = DATASTORE.prepare(query).asIterable().iterator();
    assertFalse(iterable.hasNext());

    ShardedJobStateImpl<TestTask> job = createGenericJobState();
    Entity entity = ShardedJobStateImpl.ShardedJobSerializer.toEntity(null, job);
    DATASTORE.put(entity);

    Entity singleEntity = DATASTORE.prepare(query).asSingleEntity();
    assertEquals(entity, singleEntity);
  }
}
