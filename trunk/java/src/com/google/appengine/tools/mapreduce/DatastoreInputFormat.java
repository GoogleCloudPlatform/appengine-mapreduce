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

import com.google.appengine.api.datastore.DatastoreNeedIndexException;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

/**
 * An {@code InputFormat} for reading from the App Engine datastore.
 * Currently only supports querying over an entire entity kind, for which a
 * descending index on {@value Entity#KEY_RESERVED_PROPERTY} has been defined.
 *
 * @author frew@google.com (Fred Wulff)
 */
public class DatastoreInputFormat extends InputFormat<Key, Entity> {
  private static final Logger log = Logger.getLogger(DatastoreInputFormat.class.getName());

  /**
   * Key for storing entity kind in a
   * {@link org.apache.hadoop.conf.Configuration}
   */
  public static final String ENTITY_KIND_KEY =
      "mapreduce.mapper.inputformat.datastoreinputformat.entitykind";

  /**
   * Key for storing the shard count in a {@code Configuration}
   */
  public static final String SHARD_COUNT_KEY = "mapreduce.mapper.shardcount";

  /**
   * Default number of {@link InputSplit}s to generate
   */
  public static final int DEFAULT_SHARD_COUNT = 4;

  /**
   * Returns a list of keys, starting at the root (exclusive) and ending at
   * the leaf node (inclusive), where each key's entity is the child of the
   * prior key's entity.
   */
  public static List<Key> getPath(Key leaf) {
    Key currentKey = leaf;
    LinkedList<Key> path = new LinkedList<Key>();
    do {
      path.addFirst(leaf);
      currentKey = currentKey.getParent();
    }  while (currentKey != null);
    return path;
  }

  /**
   * Generates a set of InputSplits partitioning a particular entity kind in
   * the datastore. The context's configuration must define a value for the
   * {@value #ENTITY_KIND_KEY} attribute, which will be the entity kind
   * parititioned. The partitioning happens lexicographically by key name
   * or numerically by id, as appropriate.
=======
  
  /**
   * Gets the splits for the entity kind named by the current configuration's
   * {@link #ENTITY_KIND_KEY} property, splitting lexicographically by key.
   */
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    String entityKind = context.getConfiguration().get(ENTITY_KIND_KEY);
    if (entityKind == null) {
      throw new IOException("No entity kind specified in job.");
    }
    log.info("Getting input splits for: " + entityKind);
    
    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();

    // For now we're just going to do the dumb thing and split lexicographically
    // by key.
    
    Key startKey = getStartKey(entityKind, datastoreService);
    Key endKey = getEndKey(entityKind, datastoreService);
    
    if (startKey.equals(endKey)) {
      return Arrays.asList((InputSplit) new DatastoreInputSplit(startKey, null));
    }
    
    List<Key> startPath = getPath(startKey);
    List<Key> endPath = getPath(endKey);
    int minPathLength = Math.min(startPath.size(), endPath.size());
    Iterator<Key> startPathIt = startPath.iterator();
    Iterator<Key> endPathIt = endPath.iterator();
    
    boolean foundDiff = false;
    
    // The key with the longest path such that commonAncestor is a
    // parent of both startKey and endKey.
    Key commonAncestor = null;
    
    // In practice only one pair of (startName, endName) and (startId, endId)
    // will be used.
    String startName = null;
    String endName = null;
    
    long startId = -1;
    long endId = -1;
    
    // Walk down the key paths until we find the first name or ID
    // in which the keys differ.
    for (int i = 0; i < minPathLength; i++) {
      Key currentStartKey = startPathIt.next();
      Key currentEndKey = endPathIt.next();
      if (!currentStartKey.equals(currentEndKey)) {
        foundDiff = true;
        commonAncestor = currentStartKey.getParent();
        startName = currentStartKey.getName();
        startId = currentStartKey.getId();
        endName = currentEndKey.getName();
        endId = currentEndKey.getId();
      }
    }
    
    if (!foundDiff) {
      // TODO(frew): This is the case where one entity is the parent of the
      // other. Figure out what to do.
      return Arrays.asList((InputSplit) new DatastoreInputSplit(startKey, null));
    }
    
    List<Key> splitKeys;
    int shardCount = context.getConfiguration().getInt(SHARD_COUNT_KEY, DEFAULT_SHARD_COUNT);

    // If we have names for our keys, split on those.
    if (startName != null && endName != null) {
      splitKeys = getSplitKeysFromNames(
          startName, endName, shardCount, commonAncestor, entityKind);
    } else {
      // Otherwise, use ids.
      splitKeys = getSplitKeysFromIds(startId, endId, shardCount, commonAncestor, entityKind);
    }

    return getSplitsFromSplitPoints(startKey, splitKeys);
  }

  /**
   * Get uniformly distributed keys when the entities use name-based keys.
   */
  private static List<Key> getSplitKeysFromNames(
      String startName, String endName, int shardCount, Key commonAncestor, String entityKind) {
    List<String> splitNames = StringSplitUtil.splitStrings(startName, endName, shardCount - 1);
    List<Key> splitKeys = new ArrayList<Key>(splitNames.size());
    for (String name : splitNames) {
      splitKeys.add(KeyFactory.createKey(commonAncestor, entityKind, name));
    }
    return splitKeys;
  }

  /**
   * Get uniformly distributed keys when the entities use ID-based keys.
   */
  private static List<Key> getSplitKeysFromIds(
      long startId, long endId, int shardCount, Key commonAncestor, String entityKind) {
    List<Key> splitKeys = new ArrayList<Key>(shardCount - 1);
    for (int j = 1; j < shardCount; j++) {
      long id = startId + j * (endId - startId) / shardCount;
      splitKeys.add(KeyFactory.createKey(commonAncestor, entityKind, id));
    }
    return splitKeys;
  }

  /**
   * Given a list of datastore keys, generates a set of datastore input splits such that
   * each key is the dividing point between adjacent splits.
   */
  private static List<InputSplit> getSplitsFromSplitPoints(Key startKey, List<Key> splitKeys) {
    List<InputSplit> splits = new ArrayList<InputSplit>(splitKeys.size());
    
    Key lastKey = startKey;

    for (Key currentKey : splitKeys) {
      splits.add(new DatastoreInputSplit(lastKey, currentKey));
      log.info("Added DatastoreInputSplit " +  splits.get(splits.size() - 1) + " " + lastKey 
          + " " + currentKey);
      lastKey = currentKey;
    }
    
    // Add in the final split. null is special cased so this split contains
    // [lastKey, Infinity).
    splits.add(new DatastoreInputSplit(lastKey, null));

    return splits;
  }

  private static Key getEndKey(String entityKind, DatastoreService datastoreService) 
      throws IOException {
    Query descending = new Query(entityKind)
        .addSort(Entity.KEY_RESERVED_PROPERTY, Query.SortDirection.DESCENDING)
        .setKeysOnly();
    try {
      Iterator<Entity> descendingIt 
          = datastoreService.prepare(descending).asIterator(withLimit(1));
      if (!descendingIt.hasNext()) {
        throw new IOException("No entities in the datastore query to split.");
      }
      return descendingIt.next().getKey();
    } catch (DatastoreNeedIndexException needIndexException) {
      // TODO(frew): Implement the Python guestimation routine to deal with
      // this case.
      throw new IOException("Couldn't find descending index on " + Entity.KEY_RESERVED_PROPERTY);
    }
  }

  private static Key getStartKey(String entityKind, DatastoreService datastoreService) 
      throws IOException {
    Query ascending = new Query(entityKind)
        .addSort(Entity.KEY_RESERVED_PROPERTY)
        .setKeysOnly();
    Iterator<Entity> ascendingIt 
        = datastoreService.prepare(ascending).asIterator(withLimit(1));
    if (!ascendingIt.hasNext()) {
      throw new IOException("No entities in the datastore query to split.");
    }
    return ascendingIt.next().getKey();
  }

  @Override
  public RecordReader<Key, Entity> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new DatastoreRecordReader();
  }
}
