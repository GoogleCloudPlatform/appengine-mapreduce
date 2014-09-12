// Copyright 2014 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withLimit;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.util.SplitUtil;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * Base class for Datastore inputs.
 */
abstract class BaseDatastoreInput<V, R extends BaseDatastoreInputReader<V>> extends Input<V> {

  private static final Logger logger = Logger.getLogger(BaseDatastoreInput.class.getName());

  private static final long serialVersionUID = -3939543473076385308L;

  private final String entityKind;
  private final int shardCount;
  private final String namespace;
  private final DatastoreShardStrategy spliter;

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   */
  public BaseDatastoreInput(DatastoreShardStrategy spliter, String entityKind, int shardCount) {
    this(spliter, entityKind, shardCount, null);
  }

  /**
   * @param entityKind entity kind to read from the datastore.
   * @param shardCount number of parallel shards for the input.
   */
  public BaseDatastoreInput(DatastoreShardStrategy spliter, String entityKind, int shardCount,
      String namespace) {
    Preconditions.checkArgument(shardCount > 0, "shardCount must be greater than zero.");
    this.entityKind = checkNotNull(entityKind);
    this.shardCount = shardCount;
    this.namespace = namespace;
    this.spliter = spliter;
  }

  @Override
  public List<InputReader<V>> createReaders() {
    logger.info("Getting input splits for: " + entityKind);

    DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    Key startKey = getStartKey(datastoreService);
    if (startKey == null) {
      logger.info("No data");
      return Collections.emptyList();
    }

    List<Key> splitPoints = spliter.getSplitPoints(shardCount, datastoreService);
    List<R> readers = createReadersFromSplitPoints(startKey, splitPoints);

    List<InputReader<V>> result = new ArrayList<>();
    for (List<R> readersForShard : SplitUtil.split(readers, shardCount, true)) {
      result.add(new ConcatenatingInputReader<>(readersForShard));
    }
    return result;
  }

  private List<R> createReadersFromSplitPoints(Key startKey, List<Key> splitPoints) {
    Key previousKey = startKey;
    List<R> readers = new ArrayList<>();
    for (Key currentKey : splitPoints) {
      R source = createReader(entityKind, previousKey, currentKey, namespace);
      readers.add(source);
      previousKey = currentKey;
      logger.fine("Added DatastoreInputSplit "+ source);
    }
    // Add in the final split. null is special cased so this split contains [startKey, Infinity).
    R source = createReader(entityKind, previousKey, null, namespace);
    readers.add(source);
    logger.fine("Added DatastoreInputSplit "+ source);
    return readers;
  }

  protected abstract R createReader(String kind, Key start, Key end, String namespace);

  public String getEntityKind() {
    return entityKind;
  }

  public int getShardCount() {
    return shardCount;
  }

  public String getNamespace() {
    return namespace;
  }

  private Key getStartKey(DatastoreService datastoreService) {
    Query ascending = createQuery(entityKind, namespace)
        .addSort(Entity.KEY_RESERVED_PROPERTY)
        .setKeysOnly();
    Iterator<Entity> ascendingIt = datastoreService.prepare(ascending).asIterator(withLimit(1));
    if (!ascendingIt.hasNext()) {
      return null;
    }
    return ascendingIt.next().getKey();
  }

  static Query createQuery(String kind, String namespace) {
    if (namespace == null) {
      return new Query(kind);
    }
    String ns = NamespaceManager.get();
    try {
      NamespaceManager.set(namespace);
      return new Query(kind);
    } finally {
      NamespaceManager.set(ns);
    }
  }
}
