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

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Represents an {@code InputSplit} over AppEngine datastore entities.
 * Represents the range between a start key inclusive and an end key exclusive.
 * Also stores a batch size to be used by the RecordReader.
 */
public class DatastoreInputSplit extends InputSplit implements Writable {
  /**
   * The default number of datastore entities that the
   * {@link DatastoreRecordReader} created from this split will
   * pull at a time.
   */
  private static final Logger log = Logger.getLogger(InputSplit.class.getName());

  public static final int DEFAULT_BATCH_SIZE = 50;
  
  private Key startKey;
  private Key endKey;
  private int batchSize;
  
  /**
   * Initializes a DatastoreInputSplit that includes the range from 
   * {@code startKey} (inclusive) to {@code endKey} (exclusive) and from which 
   * {@link DatastoreInputReader}s will retrieve entities in batches of 
   * {@code batchSize}.
   */
  public DatastoreInputSplit(Key startKey, Key endKey, int batchSize) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.batchSize = batchSize;
  }
  
  /**
   * Initializes a DatastoreInputSplit that includes the range from 
   * {@code startKey} (inclusive) to {@code endKey} (exclusive), using
   * a default batch size of {@value #DEFAULT_BATCH_SIZE} for fetching the
   * entities.
   */
  public DatastoreInputSplit(Key startKey, Key endKey) {
    this(startKey, endKey, DEFAULT_BATCH_SIZE);
  }
  
  // Default constructor for Writable initialization.
  DatastoreInputSplit() {
    this(null, null);
  }
  
  /**
   * Present to satisfy the InputSplit interface, but the implementation
   * is a stub that always returns 0 since the datastore doesn't provide
   * a useful definition of the length of a set of entities.
   */
  @Override
  public long getLength() {
    return 0;
  }

  /**
   * Present to satisfy the InputSplit interface, but the implementation
   * is a stub that always returns the empty array since the datastore
   * doesn't provide a useful definition of data location.
   */
  @Override
  public String[] getLocations() {
    return new String[]{};
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    startKey = KeyFactory.stringToKey(in.readUTF());
    endKey = DatastoreSerializationUtil.readKeyOrNull(in);
    batchSize = in.readInt();
    log.info("Initialized DatastoreInputSplit " 
        + (startKey != null ? startKey : "null") + " " 
        + (endKey != null ? endKey : "null"));
  }

  @Override
  public void write(DataOutput out) throws IOException {
    log.info("Writing DatastoreInputSplit " 
        + this.toString() + " "
        + (startKey != null ? startKey : "null") + " " 
        + (endKey != null ? endKey : "null"));
    out.writeUTF(KeyFactory.keyToString(startKey));
    DatastoreSerializationUtil.writeKeyOrNull(out, endKey);
    out.writeInt(batchSize);
  }
  
  /**
   * Returns the name of the datastore kind for entities in this input split.
   */
  public String getEntityKind() {
    return startKey.getKind();
  }

  /**
   * Returns the start key (inclusive) for this split.
   */
  public Key getStartKey() {
    return startKey;
  }
  
  /**
   * Returns the end key (exclusive) for this split.
   */
  public Key getEndKey() {
    return endKey;
  }
 
  public int getBatchSize() {
    return batchSize;
  }
}
