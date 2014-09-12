package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * Uses the scatter property to distribute ranges to shards.
 */
public class ScatterDatastoreShardStrategy implements DatastoreShardStrategy {

  private static final long serialVersionUID = -8775558212438082003L;

  private static final Logger logger =
      Logger.getLogger(ScatterDatastoreShardStrategy.class.getName());

  private static final String SCATTER_RESERVED_PROPERTY = Entity.SCATTER_RESERVED_PROPERTY;

  static final int SCATTER_ENTITIES_PER_SHARD = 64;
  private final String entityKind;
  private final String namespace;

  public ScatterDatastoreShardStrategy(String entityKind, String namespace) {
    this.entityKind = entityKind;
    this.namespace = namespace;
  }

  @Override
  public List<Key> getSplitPoints(int shardCount, DatastoreService datastore) {
    return chooseSplitPoints(retrieveScatterKeys(shardCount, datastore), shardCount);
  }

  private List<Key> retrieveScatterKeys(int shardCount, DatastoreService datastoreService) {
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
    int desiredNumScatterEntities =
        (shardCount * SCATTER_ENTITIES_PER_SHARD) - 1;
    Query scatter = BaseDatastoreInput.createQuery(entityKind, namespace)
        .addSort(SCATTER_RESERVED_PROPERTY).setKeysOnly();
    List<Key> result = new ArrayList<>();
    for (Entity e :
        datastoreService.prepare(scatter).asIterable(withLimit(desiredNumScatterEntities))) {
      result.add(e.getKey());
    }
    Collections.sort(result);
    logger.info(
        "Requested " + desiredNumScatterEntities + " scatter entities, retrieved " + result.size());
    return result;
  }

  @VisibleForTesting
  static List<Key> chooseSplitPoints(List<Key> scatterKeys, int numShards) {
    // Determine the number of regions per shard based on the actual number of scatter entities
    // found. The number of regions is one more than the number of keys retrieved to account for
    // the region before the first scatter entity. We ensure a minimum of 1 region per shard, since
    // this is the smallest granularity of entity space we can partition on at this stage.
    double scatterRegionsPerShard = Math.max(1.0, (double) (scatterKeys.size() + 1) / numShards);
    logger.info("Using " + scatterRegionsPerShard + " regions per shard");

    // Assuming each region contains the same number of entities (which is not true, but does as
    // the number of regions approaches infinity) assign each shard an equal number of regions
    // (rounded to the nearest scatter key).
    ArrayList<Key> splitKeys = new ArrayList<>(numShards - 1);
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
      splitKeys.add(scatterKeys.get(splitPoint));
    }
    return splitKeys;
  }
}
