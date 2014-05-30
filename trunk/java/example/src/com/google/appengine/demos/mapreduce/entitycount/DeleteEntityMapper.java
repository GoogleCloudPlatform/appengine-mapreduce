package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;

/**
 * Deletes datastore entities.
 */
public class DeleteEntityMapper extends MapOnlyMapper<Key, Void> {

  private static final long serialVersionUID = -6485226450501339416L;

  // [START datastoreMutationPool]
  private transient DatastoreMutationPool batcher;
  // [END datastoreMutationPool]

  // [START begin_and_endSlice]
  @Override
  public void beginSlice() {
    batcher = DatastoreMutationPool.create();
  }

  @Override
  public void endSlice() {
    batcher.flush();
  }
  // [END begin_and_endSlice]

  @Override
  public void map(Key key) {
    batcher.delete(key);
  }
}
