// Copyright 2014 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce.inputs;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.impl.util.SplitUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Base class for Datastore inputs.
 */
abstract class BaseDatastoreInput<V, R extends BaseDatastoreInputReader<V>> extends Input<V> {

  private static final long serialVersionUID = 8734408044047152746L;

  private static final Logger logger = Logger.getLogger(BaseDatastoreInput.class.getName());

  static final int RANGE_SEGMENTS_PER_SHARD = 64;

  private final Query originalQuery;
  private final int shardCount;

  /**
   * @param originalQuery The original query before shard splitting
   * @param shardCount number of parallel shards for the input.
   */
  public BaseDatastoreInput(Query originalQuery, int shardCount) {
    this.originalQuery = originalQuery;
    this.shardCount = shardCount;
  }

  @Override
  public List<InputReader<V>> createReaders() {
    logger.info("Getting input splits for: " + originalQuery);

    List<Query> subQueries = splitQuery();
    List<R> readers = new ArrayList<>(subQueries.size());
    for (Query query : subQueries) {
      readers.add(createReader(query));
    }

    List<InputReader<V>> result = new ArrayList<>();
    for (List<R> readersForShard : SplitUtil.split(readers, shardCount, true)) {
      result.add(new ConcatenatingInputReader<>(readersForShard));
    }
    return result;
  }

  /**
   * @return A list of input queries that together cover the same data as the original query.
   *         The number of items in the list may be either less than or greater than shardCount. If
   *         the result is smaller than shardCount fewer shards may be created. If it is larger
   *         multiple readers will be created and then passed to a concatenating reader.
   */
  private List<Query> splitQuery() {
    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
    DatastoreShardStrategy shardStrategy = new DatastoreShardStrategy(datastore);
    return shardStrategy.splitQuery(originalQuery, shardCount * RANGE_SEGMENTS_PER_SHARD);
  }

  protected abstract R createReader(Query query);

  public int getShardCount() {
    return shardCount;
  }

  static Query createQuery(String namespace, String kind) {
    String ns = NamespaceManager.get();
    try {
      if (namespace != null) {
        NamespaceManager.set(namespace);
      }
      return new Query(kind);
    } finally {
      if (namespace != null) {
        NamespaceManager.set(ns);
      }
    }
  }
}
