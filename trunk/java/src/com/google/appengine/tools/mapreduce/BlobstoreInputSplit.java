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

import com.google.appengine.api.blobstore.BlobKey;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * A writable implementation of {@code InputSplit} for BlobstoreInputFormat.
 * <p> Holds a {@code BlobKey}, {@code startIndex} and {@code endIndex}.
 *
 */
class BlobstoreInputSplit extends InputSplit implements Writable {

  private static final Logger log = Logger.getLogger(BlobstoreInputSplit.class.getName());
  private BlobKey blobKey;
  private long startIndex;
  private long endIndex;

  BlobstoreInputSplit(String key, long startIndex, long endIndex) {
    this.blobKey = new BlobKey(Preconditions.checkNotNull(key));
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    checkState();
  }

  // constructor for serialization
  BlobstoreInputSplit() {
    this("", 0, 0);
  }

  // InputSplit {
  @Override
  public long getLength() {
    return getEndIndex() - getStartIndex();
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException {
    return new String[0];
  }
  // InputSplit }

  // Writable {
  @Override
  public void write(DataOutput out) throws IOException {
    log.info("Writing BlobstoreInputSplit " + this.toString());
    out.writeUTF(blobKey.getKeyString());
    out.writeLong(startIndex);
    out.writeLong(endIndex);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blobKey = new BlobKey(Preconditions.checkNotNull(in.readUTF()));
    startIndex = in.readLong();
    endIndex = in.readLong();
    checkState();
    log.info("Initialized BlobstoreInputSplit " + this.toString());
  }
  // Writable }

  BlobKey getBlobKey() {
    return blobKey;
  }

  long getStartIndex() {
    return startIndex;
  }

  long getEndIndex() {
    return endIndex;
  }

  void checkState() {
    Preconditions.checkNotNull(blobKey);
    Preconditions.checkState(0 <= startIndex);
    Preconditions.checkState(startIndex <= endIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(blobKey, startIndex, endIndex);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    BlobstoreInputSplit that = (BlobstoreInputSplit) other;

    return (this.startIndex == that.startIndex && this.endIndex == that.endIndex
        && this.blobKey.equals(that.blobKey));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobstoreInputSplit");
    sb.append("{blobKey=").append(blobKey);
    sb.append(", startIndex=").append(startIndex);
    sb.append(", endIndex=").append(endIndex);
    sb.append('}');
    return sb.toString();
  }
}
