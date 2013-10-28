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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * An {@link Output} that writes {@link Entity}s to the Datastore.
 *
 */
public class DatastoreOutput extends Output<Entity, Void> {

  private static final long serialVersionUID = 4310542840257857134L;

  private static class DatastoreOutputWriter extends OutputWriter<Entity> {

    private static final long serialVersionUID = -3329305174118090948L;
    private final DatastoreMutationPool.Params poolParams;
    private transient DatastoreMutationPool pool;

    DatastoreOutputWriter(DatastoreMutationPool.Params poolParams) {
      this.poolParams = poolParams;
    }

    @Override
    public void beginSlice() throws IOException {
      super.beginSlice();
      DatastoreService ds = DatastoreServiceFactory.getDatastoreService();
      pool = DatastoreMutationPool.forManualFlushing(ds, poolParams);
    }

    @Override
    public void write(Entity entity) throws IOException {
      pool.put(entity);
    }

    @Override
    public void endSlice() throws IOException {
      pool.flush();
      super.endSlice();
    }

    @Override
    public void close() {
      // Nothing to do
    }
  }

  private final int numShards;
  private final DatastoreMutationPool.Params poolParams;

  public DatastoreOutput(int numShards) {
    this(numShards, DatastoreMutationPool.DEFAULT_PARAMS);
  }

  public DatastoreOutput(int numShards, DatastoreMutationPool.Params poolParams) {
    this.numShards = numShards;
    this.poolParams = checkNotNull(poolParams);
  }

  @Override
  public int getNumShards() {
    return numShards;
  }

  @Override
  public List<? extends OutputWriter<Entity>> createWriters() {
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
