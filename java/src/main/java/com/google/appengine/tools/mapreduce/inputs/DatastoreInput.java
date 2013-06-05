// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * An input to read entities of a specified kind from the datastore.
 *
 */
public class DatastoreInput extends Input<Entity> {
// --------------------------- STATIC FIELDS ---------------------------

  private static final Logger logger = Logger.getLogger(DatastoreInput.class.getName());
  private static final String SCATTER_RESERVED_PROPERTY = Entity.SCATTER_RESERVED_PROPERTY;
  private static final int SCATTER_ENTITIES_PER_SHARD = 32;
  private static final long serialVersionUID = -3939543473076385308L;

// ------------------------------ FIELDS ------------------------------

  private String entityKind;
  private int shardCount;

// --------------------------- CONSTRUCTORS ---------------------------

  public DatastoreInput() {
  }

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   */
  public DatastoreInput(String entityKind, int shardCount) {
    this.entityKind = entityKind;
    this.shardCount = shardCount;
  }

// ------------------------ IMPLEMENTING METHODS ------------------------

  @Override
  public List<? extends InputReader<Entity>> createReaders() {
    Preconditions.checkNotNull(entityKind);
    logger.info("Getting input splits for: " + entityKind);

    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    Key startKey = getStartKey(entityKind, datastoreService);
    if (startKey == null) {
      logger.info("No data");
      return Collections.emptyList();
    }

    Key lastKey = startKey;

    List<Entity> scatterEntities = retrieveScatterKeys(datastoreService);
    List<DatastoreInputReader> result = new ArrayList<DatastoreInputReader>();
    for (Key currentKey : chooseSplitPoints(scatterEntities, shardCount)) {
      DatastoreInputReader source = new DatastoreInputReader(entityKind, lastKey, currentKey);
      result.add(source);
      logger.info(
          String.format("Added DatastoreInputSplit %s %s %s", source, lastKey, currentKey));
      lastKey = currentKey;
    }

    // Add in the final split. null is special cased so this split contains
    // [lastKey, Infinity).
    result.add(new DatastoreInputReader(entityKind, lastKey, null));

    return result;
  }

// --------------------- GETTER / SETTER METHODS ---------------------

  public String getEntityKind() {
    return entityKind;
  }

  public void setEntityKind(String entityKind) {
    this.entityKind = entityKind;
  }

  public int getShardCount() {
    return shardCount;
  }

  public void setShardCount(int shardCount) {
    this.shardCount = shardCount;
  }

// -------------------------- INSTANCE METHODS --------------------------

  private List<Entity> retrieveScatterKeys(DatastoreService datastoreService) {
    // A scatter property is added to 1 out of every X entities (X is currently 512), see:
    // http://code.google.com/p/appengine-mapreduce/wiki/ScatterPropertyImplementation
    //
    // We need to determine #shards - 1 split points to divide entity space into equal shards. We
    // oversample the entities with scatter properties to get a better approximation.
    // Note: there is a region of entities before and after each scatter entity:
    //    |---*------*------*------*------*------*------*---|  * = scatter entity,   - = entity
    // so if each scatter entity represents the region following it, there is an extra region before
    // the first scatter entity. Thus we query for one less than the desired number of regions to
    // account for the this extra region before the first scatter entity
    int desiredNumScatterEntities = (shardCount * SCATTER_ENTITIES_PER_SHARD) - 1;
    Query scatter = new Query(entityKind)
      .addSort(SCATTER_RESERVED_PROPERTY)
      .setKeysOnly();
    List<Entity> scatterKeys = datastoreService.prepare(scatter).asList(
      withLimit(desiredNumScatterEntities));
    Collections.sort(scatterKeys, new Comparator<Entity>() {
      @Override
      public int compare(Entity o1, Entity o2) {
        return o1.getKey().compareTo(o2.getKey());
      }
    });
    logger.info("Requested " + desiredNumScatterEntities + " scatter entities, retrieved "
        + scatterKeys.size());
    return scatterKeys;
  }

// -------------------------- STATIC METHODS --------------------------

  protected static Iterable<Key> chooseSplitPoints(List<Entity> scatterKeys, int numShards) {
    // Determine the number of regions per shard based on the actual number of scatter entities
    // found. The number of regions is one more than the number of keys retrieved to account for
    // the region before the first scatter entity. We ensure a minimum of 1 region per shard, since
    // this is the smallest granularity of entity space we can partition on at this stage.
    double scatterRegionsPerShard = Math.max(1.0, (double) (scatterKeys.size() + 1) / numShards);
    logger.info("Using " + scatterRegionsPerShard + " regions per shard");

    // Assuming each region contains the same number of entities (which is not true, but does as
    // the number of regions approaches infinity) assign each shard an equal number of regions
    // (rounded to the nearest scatter key).
    Collection<Key> splitKeys = new ArrayList<Key>(numShards - 1);
    for (int i = 1; i < numShards; i++) {
      // Since scatterRegionsPerShard is at least one, no two values of i can produce the same
      // splitPoint. We subtract one since the array is 0-indexed, but our calculation starts w/ 1.
      int splitPoint = (int) Math.round(i * scatterRegionsPerShard) - 1;
      // Check to see if we have exhausted the scatter keys.
      if (splitPoint >= scatterKeys.size()) {
        // There were not enough regions to create the requested number of shards, fewer shards
        // will be used. This should occur iff there were too few scatter entities to start with.
        break;
      }
      splitKeys.add(scatterKeys.get(splitPoint).getKey());
    }
    return splitKeys;
  }

  private static Key getStartKey(String entityKind, DatastoreService datastoreService) {
    Query ascending = new Query(entityKind)
        .addSort(Entity.KEY_RESERVED_PROPERTY)
        .setKeysOnly();
    Iterator<Entity> ascendingIt = datastoreService.prepare(ascending).asIterator(withLimit(1));
    if (!ascendingIt.hasNext()) {
      return null;
    }
    return ascendingIt.next().getKey();
  }
}
