package com.google.appengine.tools.mapreduce.inputs;


import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Key;

import java.io.Serializable;
import java.util.List;

/**
 * Dictates how sharding should be done within {@link DatastoreInput}
 */
public interface DatastoreShardStrategy extends Serializable {

  /**
   * @return A list of keys that can be used as split points. This list may be larger or smaller
   *         than the number of shards. If it is smaller fewer shards may be created. If it is
   *         larger multiple readers will be created and then passed to a concatenating reader.
   */
  List<Key> getSplitPoints(int numShards, DatastoreService datastore);

}
