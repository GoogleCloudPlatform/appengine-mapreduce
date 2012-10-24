// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerContext;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.impl.shardedjob.InProcessShardedJobRunner;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Runs a MapReduce in the current process. Only for very small datasets. Easier to debug than a
 * parallel MapReduce.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <I> type of input values
 * @param <K> type of intermediate keys
 * @param <V> type of intermediate values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@link Output}
 */
public class InProcessMapReduce<I, K, V, O, R> {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(InProcessMapReduce.class.getName());

  static class MapResult<K, V> {
    private final List<List<KeyValue<K, V>>> mapShardOutputs;
    private final CountersImpl counters;

    public MapResult(List<List<KeyValue<K, V>>> mapShardOutputs, CountersImpl counters) {
      this.mapShardOutputs = checkNotNull(mapShardOutputs, "Null mapShardOutputs");
      this.counters = checkNotNull(counters, "Null counters");
    }

    public List<List<KeyValue<K, V>>> getMapShardOutputs() {
      return mapShardOutputs;
    }

    public CountersImpl getCounters() {
      return counters;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + "(" + mapShardOutputs + ", " + counters + ")";
    }
  }

  private final String id;
  private final MapReduceSpecification<I, K, V, O, R> mrSpec;

  public InProcessMapReduce(String id, MapReduceSpecification<I, K, V, O, R> mrSpec) {
    this.id = checkNotNull(id, "Null id");
    this.mrSpec = checkNotNull(mrSpec, "Null mrSpec");
  }

  @Override
  public String toString() {
    return "InProcessMapReduce.Impl(" + id + ")";
  }

  WorkerResult<KeyValue<K, V>> map(
      List<? extends InputReader<I>> inputs, List<? extends OutputWriter<KeyValue<K, V>>> outputs) {
    log.info("Map phase started");

    ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> tasks =
        ImmutableList.builder();
    for (int shard = 0; shard < inputs.size(); shard++) {
      WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> task = new MapShardTask<I, K, V>(id,
          shard,
          inputs.size(),
          inputs.get(shard),
          mrSpec.getMapper(),
          outputs.get(shard),
          Long.MAX_VALUE);
      tasks.add(task);
    }
    final CountersImpl counters[] = new CountersImpl[1];
    WorkerResult<KeyValue<K, V>> result = InProcessShardedJobRunner.runJob(
        tasks.build(), new AbstractWorkerController<
            WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>, KeyValue<K, V>>(
            mrSpec.getJobName() + "-map") {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 661198005749484951L;

          @Override
          public void completed(WorkerResult<KeyValue<K, V>> finalCombinedResult) {
            counters[0] = finalCombinedResult.getCounters();
          }
        });
    log.info("Map phase completed");
    return result;
  }

  List<List<KeyValue<K, List<V>>>> shuffle(
      List<List<KeyValue<K, V>>> mapperOutputs, int reduceShardCount) {
    log.info("Shuffle phase started");
    List<List<KeyValue<K, List<V>>>> out =
        Shuffling.shuffle(mapperOutputs, mrSpec.getIntermediateKeyMarshaller(), reduceShardCount);
    log.info("Shuffle phase completed");
    return out;
  }

  InputReader<KeyValue<K, ReducerInput<V>>> getReducerInputReader(
      final List<KeyValue<K, List<V>>> data) {
    return new InputReader<KeyValue<K, ReducerInput<V>>>() {
      // Not really meant to be serialized, but avoid warning.
      private static final long serialVersionUID = 310424169122893265L;
      int i = 0;

      @Override
      public Double getProgress() {
        return null;
      }

      @Override
      public KeyValue<K, ReducerInput<V>> next() {
        if (i >= data.size()) {
          throw new NoSuchElementException();
        }
        KeyValue<K, ReducerInput<V>> result =
            KeyValue.of(data.get(i).getKey(), ReducerInputs.fromIterable(data.get(i).getValue()));
        i++;
        return result;
      }
    };
  }

  MapReduceResult<R> reduce(List<List<KeyValue<K, List<V>>>> inputs, Output<O, R> output,
      List<? extends OutputWriter<O>> outputs, CountersImpl mapCounters) throws IOException {
    Preconditions.checkArgument(inputs.size() == outputs.size(), "%s reduce inputs, %s outputs",
        inputs.size(), outputs.size());
    log.info("Reduce phase started");
    ImmutableList.Builder<ReduceShardTask<K, V, O>> tasks = ImmutableList.builder();
    for (int shard = 0; shard < outputs.size(); shard++) {
      tasks.add(new ReduceShardTask<K, V, O>(id,
          shard,
          outputs.size(),
          getReducerInputReader(inputs.get(shard)),
          mrSpec.getReducer(),
          outputs.get(shard),
          Long.MAX_VALUE));
    }
    final CountersImpl[] counters = new CountersImpl[1];
    WorkerResult<O> result = InProcessShardedJobRunner.runJob(
        tasks.build(), new AbstractWorkerController<
            WorkerShardTask<KeyValue<K, ReducerInput<V>>, O, ReducerContext<O>>, O>(
            mrSpec.getJobName() + "-reduce") {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 575338448598450119L;

          @Override
          public void completed(WorkerResult<O> result) {
            counters[0] = result.getCounters();
          }
        });
    log.info("Reduce phase completed, reduce counters=" + counters[0]);
    counters[0].addAll(mapCounters);
    log.info("combined counters=" + counters[0]);
    return new ResultAndCounters<R>(output.finish(outputs), counters[0]);
  }

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  public static <I, K, V, O, R> MapReduceResult<R> runMapReduce(
      MapReduceSpecification<I, K, V, O, R> mrSpec) throws IOException {
    String mapReduceId =
        "in-process-mr-" + DATE_FORMAT.format(new Date()) + "-" + new Random().nextInt(1000000);
    InProcessMapReduce<I, K, V, O, R> mapReduce =
        new InProcessMapReduce<I, K, V, O, R>(mapReduceId, mrSpec);
    log.info(mapReduce + " started");

    List<? extends InputReader<I>> inputs = mrSpec.getInput().createReaders();
    InMemoryOutput<KeyValue<K, V>> mapOutput = InMemoryOutput.create(inputs.size());
    List<? extends OutputWriter<KeyValue<K, V>>> mapOutputs = mapOutput.createWriters();
    WorkerResult<KeyValue<K, V>> workerResult = mapReduce.map(inputs, mapOutputs);
    List<List<KeyValue<K, V>>> pairs = mapOutput.finish(mapOutputs);
    MapResult<K, V> mapResult = new MapResult<K, V>(pairs, workerResult.getCounters());

    List<? extends OutputWriter<O>> outputs = mrSpec.getOutput().createWriters();
    List<List<KeyValue<K, List<V>>>> reducerInputs =
        mapReduce.shuffle(mapResult.getMapShardOutputs(), outputs.size());
    MapReduceResult<R> result =
        mapReduce.reduce(reducerInputs, mrSpec.getOutput(), outputs, mapResult.getCounters());

    log.info(mapReduce + " finished");
    return result;
  }

}
