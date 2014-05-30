// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.outputs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes {@link Entity}s to the Datastore.
 *
 */
public class DatastoreOutput extends Output<Entity, Void> {

  private static final long serialVersionUID = 4310542840257857134L;

  public static class DatastoreOutputWriter extends OutputWriter<Entity> {

    private static final long serialVersionUID = -3329305174118090948L;
    private final DatastoreMutationPool.Params poolParams;
    private transient DatastoreMutationPool pool;

    DatastoreOutputWriter(DatastoreMutationPool.Params poolParams) {
      this.poolParams = poolParams;
    }

    @Override
    public void beginSlice() {
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      pool = DatastoreMutationPool.create(ds, poolParams);
    }

    @Override
    public void write(Entity entity) {
      pool.put(entity);
    }

    @Override
    public void endSlice() {
      pool.flush();
    }

    @Override
    public long estimateMemoryRequirement() {
      return poolParams.getBytesLimit();
    }

    @Override
    public boolean allowSliceRetry() {
      return true;
    }
  }

  private final DatastoreMutationPool.Params poolParams;

  public DatastoreOutput() {
    this(DatastoreMutationPool.DEFAULT_PARAMS);
  }

  public DatastoreOutput(DatastoreMutationPool.Params poolParams) {
    this.poolParams = checkNotNull(poolParams);
  }

  @Override
  public List<DatastoreOutputWriter> createWriters(int numShards) {
    ImmutableList.Builder<DatastoreOutputWriter> out = ImmutableList.builder();
    for (int i = 0; i < numShards; i++) {
      out.add(new DatastoreOutputWriter(poolParams));
    }
    return out.build();
  }

  /**
   * Returns null.
   */
  @Override
  public Void finish(Collection<? extends OutputWriter<Entity>> writers) {
    // Nothing to do
    return null;
  }
}
