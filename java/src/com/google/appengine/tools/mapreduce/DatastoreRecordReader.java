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
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.QueryResultIterator;
import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Logger;

import static com.google.appengine.api.datastore.FetchOptions.Builder.withChunkSize;

/**
 * DatastoreReader is a RecordReader for the AppEngine Datastore.
 * It's AppEngine compatible by way of implementing Writable.
 *
 */
public class DatastoreRecordReader extends RecordReader<Key, Entity> implements Writable {
  private static final Logger log = Logger.getLogger(DatastoreRecordReader.class.getName());

  // The split that this reader iterates over.
  private DatastoreInputSplit split;

  // The lazily generated datastore iterator for this reader.
  private QueryResultIterator<Entity> iterator;

  // The most current key and value of this reader.
  private Key currentKey;
  private Entity currentValue;

  /**
   * Implemented solely for RecordReader interface.
   */
  @Override
  public void close() {
  }

  /**
   * Completely meaningless. Implemented as part of RecordReader.
   */
  // TODO(user): Make meaningful: return some measure of
  // (currentKey - startKey) / (endKey - startKey)
  @Override
  public float getProgress() {
    return 0;
  }

  private void createIterator() {
    Preconditions.checkState(iterator == null);

    Query q = new Query(split.getEntityKind());
    if (currentKey == null) {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, FilterOperator.GREATER_THAN_OR_EQUAL,
          split.getStartKey());
    } else {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, FilterOperator.GREATER_THAN, currentKey);
    }

    if (split.getEndKey() != null) {
      q.addFilter(Entity.KEY_RESERVED_PROPERTY, FilterOperator.LESS_THAN, split.getEndKey());
    }

    q.addSort(Entity.KEY_RESERVED_PROPERTY);

    DatastoreService dsService = DatastoreServiceFactory.getDatastoreService();
    iterator = dsService.prepare(q).asQueryResultIterator(withChunkSize(split.getBatchSize()));
  }

  @Override
  public Key getCurrentKey() {
    // For consistency, if we've just deserialized and haven't iterated
    // return null for both currentKey and currentValue.
    return currentValue == null ? null : currentKey;
  }

  @Override
  public Entity getCurrentValue() {
    return currentValue;
  }

  @Override
  public boolean nextKeyValue() {
    if (iterator == null) {
      createIterator();
    }

    if (!iterator.hasNext()) {
      return false;
    }

    Entity entity = iterator.next();
    currentKey = entity.getKey();
    currentValue = entity;

    return true;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    currentKey = DatastoreSerializationUtil.readKeyOrNull(in);
    log.fine("DatastoreRecordReader reconstituted: " + currentKey);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    log.fine("DatastoreRecordReader serialization: " + currentKey);
    DatastoreSerializationUtil.writeKeyOrNull(out, currentKey);
  }

  /**
   * Initialize the reader.
   *
   * @throws IOException if the split provided isn't a DatastoreInputSplit
   */
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    Preconditions.checkNotNull(split);
    if (!(split instanceof DatastoreInputSplit)) {
      throw new IOException(
          getClass().getName() + " initialized with non-DatastoreInputSplit");
    }
    this.split = (DatastoreInputSplit) split;
  }
}
