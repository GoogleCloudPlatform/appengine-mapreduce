// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConcatenatingInputReader;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInputReader;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInputReader;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Defines the way the data is read in by the Sort phase. This consists of logically concatenating
 * multiple GoogleCloudStorage files to form a single input of KeyValue pairs. The sorter does not
 * care what the individual values are so they are not deserialized.
 *
 */
public class GoogleCloudStorageSortInput extends Input<KeyValue<ByteBuffer, ByteBuffer>> {

  private static final long serialVersionUID = -3995775161471778634L;

  private List<GoogleCloudStorageFileSet> filenames;

  private static class ReaderCreatorImpl implements
      ConcatenatingInputReader.ReaderCreator<KeyValue<ByteBuffer, ByteBuffer>> {

    private static final long serialVersionUID = 2949471237868897860L;
    private GoogleCloudStorageFileSet filenames;
    private int index;

    ReaderCreatorImpl(GoogleCloudStorageFileSet filenames) {
      this.filenames = Preconditions.checkNotNull(filenames);
      this.index = -1;
    }

    @Override
    public InputReader<KeyValue<ByteBuffer, ByteBuffer>> createNextReader() {
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      Marshaller<KeyValue<ByteBuffer, ByteBuffer>> marshaller =
          new KeyValueMarshaller<ByteBuffer, ByteBuffer>(identity, identity);
      index++;
      if (index >= filenames.getNumFiles()) {
        return null;
      }
      GcsFilename file = filenames.getFile(index);
      GoogleCloudStorageLevelDbInputReader in = new GoogleCloudStorageLevelDbInputReader(file,
          MapReduceConstants.INPUT_BUFFER_SIZE);
      return new UnmarshallingInputReader<KeyValue<ByteBuffer, ByteBuffer>>(in, marshaller);
    }

    @Override
    public int estimateTotalNumberOfReaders() {
      return filenames.getNumFiles();
    }
  }

  public GoogleCloudStorageSortInput(List<GoogleCloudStorageFileSet> filenames) {
    this.filenames = checkNotNull(filenames, "Null filenames");
  }

  @Override
  public List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> createReaders() {
    Marshaller<ByteBuffer> idenity = Marshallers.getByteBufferMarshaller();
    ImmutableList.Builder<InputReader<KeyValue<ByteBuffer, ByteBuffer>>> out =
        ImmutableList.builder();
    for (GoogleCloudStorageFileSet shard : filenames) {
      out.add(new ConcatenatingInputReader<KeyValue<ByteBuffer, ByteBuffer>>(
          new ReaderCreatorImpl(shard)));
    }
    return out.build();
  }

}
