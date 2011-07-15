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

/**
 * A key for mapreduce mapper over Blobstore.
 *
 */
public final class BlobstoreRecordKey {

  private final BlobKey blobKey;
  private final long offset;

  BlobstoreRecordKey(BlobKey blobKey, long offset) {
    Preconditions.checkNotNull(blobKey);
    Preconditions.checkArgument(offset >= 0);
    this.blobKey = blobKey;
    this.offset = offset;
  }

  public BlobKey getBlobKey() {
    return blobKey;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    BlobstoreRecordKey that = (BlobstoreRecordKey) other;

    return offset == that.offset && blobKey.equals(that.blobKey);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(blobKey, offset);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlobstoreRecordKey");
    sb.append("{blobKey=").append(blobKey);
    sb.append(", offset=").append(offset);
    sb.append('}');
    return sb.toString();
  }
}
