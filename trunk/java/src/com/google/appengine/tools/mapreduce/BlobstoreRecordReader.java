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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.CountingInputStream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Iterator;

/**
 * BlobstoreRecordReader is a RecordReader for the AppEngine Blobstore.
 * It's AppEngine compatible by way of implementing Writable.
 *
 * @author idk@google.com (Igor Kushnirskiy)
 */
class BlobstoreRecordReader extends RecordReader<BlobstoreRecordKey, byte[]> implements Writable {

  private static final int MIN_BUFFER_SIZE = 1024 * 256;

  private static final byte DEFAULT_TERMINATOR = '\n';

  @VisibleForTesting
  static interface InputStreamFactory {
    InputStream getInputStream(BlobKey blobKey, long offset) throws IOException;
  }

  @VisibleForTesting
  static interface InputStreamIteratorFactory {
    Iterator<InputStreamIterator.OffsetRecordPair> getInputStreamIterator(
        CountingInputStream input, long length, boolean isLeadingSplit, byte terminator);
  }

  // The split that this reader iterates over.
  private BlobstoreInputSplit split;

  private CountingInputStream input;

  private long offset = 0;

  private byte terminator;

  private InputStreamFactory inputStreamFactory =
      new InputStreamFactory() {

        private final BlobstoreReadableByteChannelFactory readableByteChannelFactory =
            new BlobstoreReadableByteChannelFactory();

        @Override
        public InputStream getInputStream(BlobKey blobKey, long offset) throws IOException {
          return Channels.newInputStream(
              readableByteChannelFactory.getReadableByteChannel(blobKey, offset));
        }
      };

  private InputStreamIteratorFactory inputStreamIteratorFactory =
      new InputStreamIteratorFactory() {
        @Override
        public Iterator<InputStreamIterator.OffsetRecordPair> getInputStreamIterator(
            CountingInputStream input, long length, boolean isLeadingSplit, byte terminator) {
          return new InputStreamIterator(input, length, isLeadingSplit, terminator);
        }
      };

  private Iterator<InputStreamIterator.OffsetRecordPair> recordIterator;

  // RecordReader {
  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(split);
    if (!(split instanceof BlobstoreInputSplit)) {
      throw new IOException(
          getClass().getName() + " initialized with non-BlobstoreInputSplit");
    }

    this.split = (BlobstoreInputSplit) split;
    int intTerminator =
        context.getConfiguration().getInt(BlobstoreInputFormat.TERMINATOR, DEFAULT_TERMINATOR);
    Preconditions.checkState(Byte.MIN_VALUE <= intTerminator && intTerminator <= Byte.MAX_VALUE,
        BlobstoreInputFormat.TERMINATOR + " is not in [" + Byte.MIN_VALUE + ", " + Byte.MAX_VALUE
            + "] range.");
    terminator = (byte) intTerminator;
    input = getInputStream(this.split, offset);
    recordIterator = getInputStreamIterator(input, this.split, offset, terminator);
  }

  private Iterator<InputStreamIterator.OffsetRecordPair> getInputStreamIterator(
      CountingInputStream input, BlobstoreInputSplit split, long offset, byte terminator) {
    return inputStreamIteratorFactory
        .getInputStreamIterator(input, split.getLength() - offset,
            split.getStartIndex() != 0 && offset == 0, terminator);
  }

  private CountingInputStream getInputStream(BlobstoreInputSplit split, long offset)
      throws IOException {
    int bufferSize = (int) split.getLength();
    bufferSize *= 1.5;
    bufferSize = Math.max(bufferSize, MIN_BUFFER_SIZE);

    return new CountingInputStream(new BufferedInputStream(inputStreamFactory.getInputStream(
        split.getBlobKey(), split.getStartIndex() + offset), bufferSize));
  }

  @Override
  public boolean nextKeyValue() {
    return recordIterator.hasNext();
  }

  @Override
  public BlobstoreRecordKey getCurrentKey() {
    return new BlobstoreRecordKey(split.getBlobKey(),
        split.getStartIndex() + offset + recordIterator.next().getOffset());
  }

  @Override
  public byte[] getCurrentValue() {
    return recordIterator.next().getRecord();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return (offset + input.getCount()) / split.getLength();
  }

  @Override
  public void close() throws IOException {
    // do nothing
  }
  // RecordReader }

  // Writable {
  @Override
  public void write(DataOutput out) throws IOException {
    long newOffset = (input == null) ? 0 : input.getCount();
    out.writeLong(offset + newOffset);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    offset = in.readLong();
    input = getInputStream(split, offset);
    recordIterator = getInputStreamIterator(input, split, offset, terminator);
  }


  // Writable }

  @VisibleForTesting
  void setInputStreamFactory(InputStreamFactory inputStreamFactory) {
    this.inputStreamFactory = Preconditions.checkNotNull(inputStreamFactory);
  }

  @VisibleForTesting
  void setInputStreamIteratorFactory(InputStreamIteratorFactory inputStreamIteratorFactory) {
    this.inputStreamIteratorFactory = Preconditions.checkNotNull(inputStreamIteratorFactory);
  }
}
