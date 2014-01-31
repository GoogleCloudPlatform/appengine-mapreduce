package com.google.appengine.demos.mapreduce.entitycount;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.DatastoreMutationPool;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * Deletes Entities for datastore provided they match the specified {@link Predicate}.
 * If no {@link Predicate} is provided all entities are assumed to match.
 */
public class DeleteEntityMapper extends Mapper<Entity, Void, Void> {

  private final Predicate<Entity> shouldDelete;
  private transient DatastoreMutationPool batcher;

  public DeleteEntityMapper(Predicate<Entity> shouldDelete) {
    if (shouldDelete == null) {
      this.shouldDelete = Predicates.alwaysTrue();
    } else {
      this.shouldDelete = shouldDelete;
    }
  }

  @Override
  public void beginSlice() {
    batcher = DatastoreMutationPool.create();
  }

  @Override
  public void endSlice() {
    batcher.flush();
  }

  @Override
  public void map(Entity value) {
    if (shouldDelete.apply(value)) {
      batcher.delete(value.getKey());
    }
  }
}