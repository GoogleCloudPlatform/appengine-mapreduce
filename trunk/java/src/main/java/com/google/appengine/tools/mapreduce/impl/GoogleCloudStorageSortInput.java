// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConcatenatingInputReader;
import com.google.appengine.tools.mapreduce.inputs.ForwardingInputReader;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInputReader;
import com.google.appengine.tools.mapreduce.inputs.UnmarshallingInputReader;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the way the data is read in by the Sort phase. This consists of logically concatenating
 * multiple GoogleCloudStorage files to form a single input of KeyValue pairs. The sorter does not
 * care what the individual values are so they are not deserialized.
 *
 */
public class GoogleCloudStorageSortInput extends Input<KeyValue<ByteBuffer, ByteBuffer>> {

  private static final long serialVersionUID = -3995775161471778634L;

  private final FilesByShard files;

  private static class ReaderImpl extends ForwardingInputReader<KeyValue<ByteBuffer, ByteBuffer>> {

    private static final long serialVersionUID = 3310058647644865812L;

    private final InputReader<KeyValue<ByteBuffer, ByteBuffer>> reader;

    private ReaderImpl(GcsFilename file) {
      Marshaller<ByteBuffer> identity = Marshallers.getByteBufferMarshaller();
      Marshaller<KeyValue<ByteBuffer, ByteBuffer>> marshaller =
          new KeyValueMarshaller<>(identity, identity);
      GoogleCloudStorageLevelDbInputReader in =
          new GoogleCloudStorageLevelDbInputReader(file, DEFAULT_IO_BUFFER_SIZE);
      reader = new UnmarshallingInputReader<>(in, marshaller);
    }

    @Override
    protected InputReader<KeyValue<ByteBuffer, ByteBuffer>> getDelegate() {
      return reader;
    }
  }

  public GoogleCloudStorageSortInput(FilesByShard files) {
    this.files = checkNotNull(files, "Null files");
  }

  @Override
  public List<? extends InputReader<KeyValue<ByteBuffer, ByteBuffer>>> createReaders() {
    ImmutableList.Builder<InputReader<KeyValue<ByteBuffer, ByteBuffer>>> out =
        ImmutableList.builder();
    for (int shard = 0; shard < files.getShardCount(); shard++) {
      List<ReaderImpl> readersForShard = new ArrayList<>();
      for (GcsFilename file : files.getFilesForShard(shard).getFiles()) {
        readersForShard.add(new ReaderImpl(file));
      }
      out.add(new ConcatenatingInputReader<>(readersForShard));
    }
    return out.build();
  }
}
