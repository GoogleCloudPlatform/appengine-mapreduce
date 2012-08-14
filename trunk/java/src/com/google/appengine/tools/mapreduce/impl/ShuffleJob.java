// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.handlers.MapReduceServletImpl;
import com.google.appengine.tools.mapreduce.impl.util.FileUtil;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Job2;
import com.google.appengine.tools.pipeline.PromisedValue;
import com.google.appengine.tools.pipeline.Value;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 */
public class ShuffleJob<K, V, O>
    // We don't need the CountersImpl part of the MapResult input here but we
    // accept it to avoid needing an adapter job to connect this job to MapJob's
    // result.
    extends Job1<ShuffleResult<K, V, O>, ResultAndCounters<List<AppEngineFile>>> {
  private static final long serialVersionUID = 394826723385510650L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(ShuffleJob.class.getName());

  private static final FileService FILE_SERVICE = FileServiceFactory.getFileService();

  private final String mrJobId;
  private final MapReduceSpecification<?, K, V, O, ?> mrSpec;
  private final MapReduceSettings settings;

  public ShuffleJob(String mrJobId,
      MapReduceSpecification<?, K, V, O, ?> mrSpec,
      MapReduceSettings settings) {
    this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
    this.settings = checkNotNull(settings, "Null settings");
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "(" + mrJobId + ")";
  }

  @Override public Value<ShuffleResult<K, V, O>> run(
      ResultAndCounters<List<AppEngineFile>> mapResult) {
    List<AppEngineFile> mapOutputs = mapResult.getOutputResult();
    List<? extends OutputWriter<O>> reducerWriters = Util.createWriters(mrSpec.getOutput());
    int reduceShardCount = reducerWriters.size();
    if (mapOutputs.isEmpty()) {
      // We can't short circuit the reduce stage entirely, since the reducer
      // might still generate output or change counters in beginShard() or
      // similar.  But we can skip the shuffling.
      return immediate(
          new ShuffleResult<K, V, O>(ImmutableList.<AppEngineFile>of(),
              reducerWriters,
              Util.createReaders(NoInput.<KeyValue<K, ReducerInput<V>>>create(reduceShardCount))));
    }
    ImmutableList.Builder<AppEngineFile> b = ImmutableList.builder();
    for (int i = 0; i < reduceShardCount; i++) {
      try {
        b.add(FILE_SERVICE.createNewBlobFile(MapReduceConstants.REDUCE_INPUT_MIME_TYPE,
                mrJobId + ": reduce input, shard " + i));
      } catch (IOException e) {
        // TODO(ohler): retry
        throw new RuntimeException(this + ": IOException creating reduce input file " + i, e);
      }
    }
    List<AppEngineFile> reduceInputs = b.build();
    ShuffleResult<K, V, O> shuffleResult = new ShuffleResult<K, V, O>(
        reduceInputs,
        reducerWriters,
        Util.createReaders(
            new IntermediateInput<K, V>(reduceInputs,
                mrSpec.getIntermediateKeyMarshaller(),
                mrSpec.getIntermediateValueMarshaller())));
    if (new ShuffleService().isAvailable()) {
      PromisedValue<String> shuffleError = newPromise(String.class);
      new ShuffleService().shuffle("Shuffle-for-MR-" + mrJobId,
          mapOutputs,
          reduceInputs,
          new ShuffleService.ShuffleCallback(
              settings.getBaseUrl() + MapReduceServletImpl.SHUFFLE_CALLBACK_PATH
              + "?promiseHandle=" + shuffleError.getHandle()
          )
          // The shuffler API doesn't let us set parameters for POST, and the
          // task queue doesn't let us use a query string, either.
          .setMethod("GET")
          .setQueue(settings.getControllerQueueName()));
      return futureCall(new WaitForShuffleJob<K, V, O>(mrJobId),
          immediate(shuffleResult), shuffleError, Util.jobSettings(settings));
    } else {
      // Do shuffling as a separate job to make it visible in the Pipeline UI
      // that shuffling is in-memory.
      return futureCall(new InMemoryShuffleJob<K, V, O>(mrJobId, mrSpec),
          immediate(mapOutputs),
          immediate(reduceInputs), immediate(shuffleResult),
          Util.jobSettings(settings));
    }
  }

  private static class WaitForShuffleJob<K, V, O>
      extends Job2<ShuffleResult<K, V, O>, ShuffleResult<K, V, O>, String> {
    private static final long serialVersionUID = 308217691163421115L;

    private final String mrJobId;

    private WaitForShuffleJob(String mrJobId) {
      this.mrJobId = checkNotNull(mrJobId, "Null mrJobId");
    }

    @Override public String toString() {
      return getClass().getSimpleName() + "(" + mrJobId + ")";
    }

    @Override public Value<ShuffleResult<K, V, O>> run(
        ShuffleResult<K, V, O> shuffleResult, String shuffleError) {
      if (shuffleError != null) {
        // TODO(ohler): abort pipeline rather than retrying
        throw new RuntimeException("Shuffler signalled an error: " + shuffleError);
      }
      for (AppEngineFile file : shuffleResult.getReducerInputFiles()) {
        FileUtil.ensureFinalized(file);
      }
      return immediate(shuffleResult);
    }
  }

}
