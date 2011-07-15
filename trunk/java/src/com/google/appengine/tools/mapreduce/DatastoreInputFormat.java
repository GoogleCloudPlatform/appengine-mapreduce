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

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;

/**
 * An {@code InputFormat} for reading from the App Engine datastore.
 * Currently only supports querying over an entire entity kind, for which a
 * descending index on {@value Entity#KEY_RESERVED_PROPERTY} has been defined.
 *
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

  // TODO(user): Move to using Entity.SCATTER_RESERVED_PROPERTY when
  // 1.4.2 comes out.
  public static final String SCATTER_RESERVED_PROPERTY = "__scatter__";

  // For each shard requested, we request SCATTER_OVERSAMPLE_FACTOR keys
  // with the scatter property, then sort and pick every
  // SCATTER_OVERSAMPLE_FACTORth of the returned properties.
  public static final int SCATTER_OVERSAMPLE_FACTOR = 32;

  /**
   * Generates a set of InputSplits partitioning a particular entity kind in
   * the datastore. The context's configuration must define a value for the
   * {@value #ENTITY_KIND_KEY} attribute, which will be the entity kind
   * partitioned, as well as a value for {@value #SHARD_COUNT_KEY} attribute,
   * which will be the maximum number of shards to split into.
   */
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    String entityKind = context.getConfiguration().get(ENTITY_KIND_KEY);
    if (entityKind == null) {
      throw new IOException("No entity kind specified in job.");
    }
    log.info("Getting input splits for: " + entityKind);

    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    Key startKey = getStartKey(entityKind, datastoreService);
    if (startKey == null) {
      return new ArrayList<InputSplit>();
    }

    int shardCount = context.getConfiguration().getInt(SHARD_COUNT_KEY, DEFAULT_SHARD_COUNT);
    int desiredScatterResultCount = shardCount * SCATTER_OVERSAMPLE_FACTOR;
    // NB(frew): If scatter doesn't exist (as in the 1.4.0 dev_appserver)
    // then we'll just end up with one split. This seems reasonable.
    Query scatter = new Query(entityKind)
        .addSort(SCATTER_RESERVED_PROPERTY)
        .setKeysOnly();
    List<Entity> scatterList = datastoreService.prepare(scatter).asList(
        withLimit(desiredScatterResultCount));
    Collections.sort(scatterList, new Comparator<Entity>() {
      public int compare(Entity e1, Entity e2) {
        return e1.getKey().compareTo(e2.getKey());
      }
    });

    List<Key> splitKeys = new ArrayList(shardCount);
    // Possibly use a lower oversampling factor if there aren't enough scatter
    // property-containing entities to fill out the list.
    int usedOversampleFactor = Math.max(1, scatterList.size() / shardCount);
    log.info("Requested " + desiredScatterResultCount + " scatter entities. Got "
        + scatterList.size() + " so using oversample factor " + usedOversampleFactor);
    // We expect the points to be uniformly randomly distributed. So we
    // act like the first point is the start key (which we alread know) and
    // omit it. This converges on correct as the number of samples goes
    // to infinity.
    for (int i = 1; i < shardCount; i++) {
      // This can happen if we don't have as many scatter properties as we want.
      if (i * usedOversampleFactor >= scatterList.size()) {
        break;
      }
      splitKeys.add(scatterList.get(i * usedOversampleFactor).getKey());
    }

    return getSplitsFromSplitPoints(startKey, splitKeys);
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

  private static Key getStartKey(String entityKind, DatastoreService datastoreService)
      throws IOException {
    Query ascending = new Query(entityKind)
        .addSort(Entity.KEY_RESERVED_PROPERTY)
        .setKeysOnly();
    Iterator<Entity> ascendingIt
        = datastoreService.prepare(ascending).asIterator(withLimit(1));
    if (!ascendingIt.hasNext()) {
      return null;
    }
    return ascendingIt.next().getKey();
  }

  @Override
  public RecordReader<Key, Entity> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new DatastoreRecordReader();
  }
}
