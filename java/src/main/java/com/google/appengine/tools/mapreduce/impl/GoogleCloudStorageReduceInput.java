// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.GoogleCloudStorageFileSet;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInput;
import com.google.appengine.tools.mapreduce.inputs.PeekingInputReader;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Defines the way the data is read in by the reducer. This consists of a number of files in GCS
 * where the content is sorted {@link KeyValue} of K and a list of V written LevelDb format.
 * {@link KeyValuesMarshaller} to unmarshall the individual records. To maintain the sorted order a
 * {@link MergingReader} is used.
 *
 *
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 */
public class GoogleCloudStorageReduceInput<K, V> extends Input<KeyValue<K, Iterator<V>>> {

  private static final long serialVersionUID = 8877197357362096382L;
  private Marshaller<K> keyMarshaller;
  private Marshaller<V> valueMarshaller;
  private List<GoogleCloudStorageFileSet> allReducerFileSets;

  public GoogleCloudStorageReduceInput(List<GoogleCloudStorageFileSet> files,
      Marshaller<K> keyMarshaller, Marshaller<V> valueMarshaller) {
    this.allReducerFileSets = checkNotNull(files, "Null files");
    this.keyMarshaller = checkNotNull(keyMarshaller, "Null keyMarshaller");
    this.valueMarshaller = checkNotNull(valueMarshaller, "Null valueMarshaller");
  }

  @Override
  public List<? extends InputReader<KeyValue<K, Iterator<V>>>> createReaders() {
    Marshaller<KeyValue<ByteBuffer, Iterator<V>>> marshaller =
        Marshallers.getKeyValuesMarshaller(Marshallers.getByteBufferMarshaller(), valueMarshaller);
    ImmutableList.Builder<MergingReader<K, V>> result = ImmutableList.builder();
    for (GoogleCloudStorageFileSet reducerInputFileSet : allReducerFileSets) {
      result.add(createReaderForShard(marshaller, reducerInputFileSet));
    }
    return result.build();
  }

  /**
   * Create a {@link MergingReader} that combines all the input files the reducer to provide a
   * global sort over all data for the shard.
   *
   *  (There are multiple input files in the event that the data didn't fit into the sorter's
   * memory)
   *
   * A {@link MergingReader} is used to combine contents while maintaining key-order. This requires
   * a {@link PeekingInputReader}s to preview the next item of input.
   *
   * @returns a reader producing key-sorted input for a shard.
   */
  private MergingReader<K, V> createReaderForShard(
      Marshaller<KeyValue<ByteBuffer, Iterator<V>>> marshaller,
      GoogleCloudStorageFileSet reducerInputFileSet) {
    ArrayList<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>> inputFiles =
        new ArrayList<PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>>();

    GoogleCloudStorageLevelDbInput reducerInput = new GoogleCloudStorageLevelDbInput(
        reducerInputFileSet, MapReduceConstants.INPUT_BUFFER_SIZE);
    for (InputReader<ByteBuffer> in : reducerInput.createReaders()) {
      inputFiles.add(new PeekingInputReader<KeyValue<ByteBuffer, Iterator<V>>>(in, marshaller));
    }
    return new MergingReader<K, V>(inputFiles, keyMarshaller);
  }


}
