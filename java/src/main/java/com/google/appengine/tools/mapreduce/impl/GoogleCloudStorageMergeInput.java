// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.MapReduceConstants.DEFAULT_IO_BUFFER_SIZE;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.ConcatenatingInputReader;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Defines the way the data is read in by the merger. This consists of merging GCS files that
 * contain sorted {@link KeyValue} values in LevelDb format. To maintain the sorted order a
 * {@link MergingReader} is used.
 * (Note the returned sequence will go backwards, see{@link #createReaders()})
 *
 */
public class GoogleCloudStorageMergeInput extends
    Input<KeyValue<ByteBuffer, Iterator<ByteBuffer>>> {

  private static final long serialVersionUID = 3532660814044212575L;
  private final FilesByShard filesByShard;
  private final Integer mergeFanin;

  public GoogleCloudStorageMergeInput(FilesByShard files, int mergeFanin) {
    this.filesByShard = checkNotNull(files, "Null files");
    this.mergeFanin = mergeFanin;
  }

  /**
   * Creates multiple merging readers for each shard using {@link #createReaderForShard} below.
   * These are combined into a single reader via concatenation. The resulting input stream will be
   * in order except when the input switches from one merging reader to the next the key will very
   * likely go backwards.
   */
  @Override
  public List<? extends InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> createReaders() {
    Marshaller<ByteBuffer> byteBufferMarshaller = Marshallers.getByteBufferMarshaller();
    Marshaller<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> marshaller =
        Marshallers.getKeyValuesMarshaller(byteBufferMarshaller, byteBufferMarshaller);
    ImmutableList.Builder<InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> result =
        ImmutableList.builder();
    for (int shard = 0; shard < filesByShard.getShardCount(); shard++) {
      List<InputReader<KeyValue<ByteBuffer, Iterator<ByteBuffer>>>> readers = new ArrayList<>();
      for (List<String> group : Lists.partition(filesByShard.getFilesForShard(shard).getFileNames(),
          mergeFanin)) {
        GoogleCloudStorageFileSet fileSet =
            new GoogleCloudStorageFileSet(filesByShard.getBucket(), group);
        readers.add(createReaderForShard(marshaller, fileSet));
      }
      result.add(new ConcatenatingInputReader<>(readers));
    }
    return result.build();
  }

  /**
   * Create a {@link MergingReader} that combines all the input files and maintain sort order.
   *
   *  (There are multiple input files in the event that the data didn't fit into the sorter's
   * memory)
   *
   * A {@link MergingReader} is used to combine contents while maintaining key-order. This requires
   * a {@link PeekingInputReader}s to preview the next item of input.
   *
   * @returns a reader producing key-sorted input for a shard.
   */
  private MergingReader<ByteBuffer, ByteBuffer> createReaderForShard(
      Marshaller<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> marshaller,
      GoogleCloudStorageFileSet inputFileSet) {
    ArrayList<PeekingInputReader<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>>> inputFiles =
        new ArrayList<>();
    GoogleCloudStorageLevelDbInput reducerInput =
        new GoogleCloudStorageLevelDbInput(inputFileSet, DEFAULT_IO_BUFFER_SIZE);
    for (InputReader<ByteBuffer> in : reducerInput.createReaders()) {
      inputFiles.add(new PeekingInputReader<>(in, marshaller));
    }
    return new MergingReader<>(inputFiles, Marshallers.getByteBufferMarshaller(), false);
  }
}