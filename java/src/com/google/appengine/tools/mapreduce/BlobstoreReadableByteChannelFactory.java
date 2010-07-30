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

import com.google.appengine.api.blobstore.BlobInfoFactory;
import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.blobstore.BlobstoreService;
import com.google.appengine.api.blobstore.BlobstoreServiceFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A factory for ReadableByteChannel view of a Blobstore.
 *
 * @author idk@google.com (Igor Kushnirskiy)
 */
class BlobstoreReadableByteChannelFactory {

  private Function<BlobKey, Long> blobKeyToSize = new Function<BlobKey, Long>() {
    @Override
    public Long apply(BlobKey key) {
      return new BlobInfoFactory().loadBlobInfo(key).getSize();
    }
  };

  private BlobstoreService blobstoreService = BlobstoreServiceFactory.getBlobstoreService();

  ReadableByteChannel getReadableByteChannel(BlobKey blobKey, long offset) {
    return new BlobstoreReadableByteChannel(blobstoreService, blobKey, blobKeyToSize.apply(blobKey),
        offset);
  }

  @VisibleForTesting
  void setBlobKeyToSize(Function<BlobKey, Long> blobKeyToSize) {
    this.blobKeyToSize = Preconditions.checkNotNull(blobKeyToSize);
  }

  @VisibleForTesting
  void setBlobstoreService(BlobstoreService blobstoreService) {
    this.blobstoreService = Preconditions.checkNotNull(blobstoreService);
  }

  private static class BlobstoreReadableByteChannel implements ReadableByteChannel {

    private final BlobstoreService blobstoreService;
    private final BlobKey blobKey;
    private final long size;

    private long offset;

    BlobstoreReadableByteChannel(BlobstoreService blobstoreService, BlobKey blobKey,
        long size, long offset) {
      this.blobstoreService = Preconditions.checkNotNull(blobstoreService);
      this.blobKey = Preconditions.checkNotNull(blobKey);
      this.size = size;
      Preconditions.checkArgument(offset >= 0 && offset < size);
      this.offset = offset;
    }


    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (offset >= size) {
        return -1;
      }
      long endIndex = Math.min(size, offset +
          Math.min(dst.remaining(),
              BlobstoreService.MAX_BLOB_FETCH_SIZE));
      // because of http://code.google.com/p/googleappengine/issues/detail?id=3445
      // the size of the array returned by fetchData may be one byte larger than we ask.
      // as a workaround we are handling the length ourselves.
      // TODO(idk): remove the workaround when the bug is fixed.
      byte[] fetched = blobstoreService.fetchData(blobKey, offset, endIndex);
      int length = (int) (endIndex - offset);
      dst.put(fetched, 0, length);
      offset = endIndex;
      return length;
    }

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public void close() throws IOException {
      // do nothing
    }
  }
}
