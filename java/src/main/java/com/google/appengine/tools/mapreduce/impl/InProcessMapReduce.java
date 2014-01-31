// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.MapperContext;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerContext;
import com.google.appengine.tools.mapreduce.impl.shardedjob.InProcessShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Iterator;
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

  private static final Logger log = Logger.getLogger(InProcessMapReduce.class.getName());

  static class MapResult<K, V> {
    private final List<List<KeyValue<K, V>>> mapShardOutputs;
    private final Counters counters;

    public MapResult(List<List<KeyValue<K, V>>> mapShardOutputs, Counters counters) {
      this.mapShardOutputs = checkNotNull(mapShardOutputs, "Null mapShardOutputs");
      this.counters = checkNotNull(counters, "Null counters");
    }

    public List<List<KeyValue<K, V>>> getMapShardOutputs() {
      return mapShardOutputs;
    }

    public Counters getCounters() {
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

  MapReduceResultImpl<List<List<KeyValue<K, V>>>> map(
      List<? extends InputReader<I>> inputs, InMemoryOutput<KeyValue<K, V>> output) {
    log.info("Map phase started");

    ImmutableList.Builder<WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> tasks =
        ImmutableList.builder();
    List<? extends OutputWriter<KeyValue<K, V>>> writers = output.createWriters();
    for (int shard = 0; shard < inputs.size(); shard++) {
      WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> task = new MapShardTask<>(
          id, shard, inputs.size(), inputs.get(shard), mrSpec.getMapper(), writers.get(shard),
          Long.MAX_VALUE);
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>>(
            mrSpec.getJobName() + "-map") {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 661198005749484951L;

          @Override
          public void failed(Status status) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void completed(
              List<? extends WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>>> tasks) {
            for (WorkerShardTask<I, KeyValue<K, V>, MapperContext<K, V>> task : tasks) {
              counters.addAll(task.getContext().getCounters());
            }
          }
        });
    log.info("Map phase completed");
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(writers), counters);
  }

  List<List<KeyValue<K, List<V>>>> shuffle(
      List<List<KeyValue<K, V>>> mapperOutputs, int reduceShardCount) {
    log.info("Shuffle phase started");
    List<List<KeyValue<K, List<V>>>> out =
        Shuffling.shuffle(mapperOutputs, mrSpec.getIntermediateKeyMarshaller(), reduceShardCount);
    log.info("Shuffle phase completed");
    return out;
  }

  InputReader<KeyValue<K, Iterator<V>>> getReducerInputReader(
      final List<KeyValue<K, List<V>>> data) {
    return new InputReader<KeyValue<K, Iterator<V>>>() {
      // Not really meant to be serialized, but avoid warning.
      private static final long serialVersionUID = 310424169122893265L;
      int i = 0;

      @Override
      public Double getProgress() {
        return null;
      }

      @Override
      public KeyValue<K, Iterator<V>> next() {
        if (i >= data.size()) {
          throw new NoSuchElementException();
        }
        KeyValue<K, Iterator<V>> result =
            KeyValue.of(data.get(i).getKey(), data.get(i).getValue().iterator());
        i++;
        return result;
      }
    };
  }

  MapReduceResult<R> reduce(List<List<KeyValue<K, List<V>>>> inputs, Output<O, R> output,
      Counters mapCounters) throws IOException {
    List<? extends OutputWriter<O>> outputs = output.createWriters();
    Preconditions.checkArgument(inputs.size() == outputs.size(), "%s reduce inputs, %s outputs",
        inputs.size(), outputs.size());
    log.info("Reduce phase started");
    ImmutableList.Builder<WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>> tasks =
        ImmutableList.builder();
    for (int shard = 0; shard < outputs.size(); shard++) {
      WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>> task =
          new ReduceShardTask<>(id, shard, outputs.size(), getReducerInputReader(inputs.get(shard)),
              mrSpec.getReducer(), outputs.get(shard), Long.MAX_VALUE);
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();
    counters.addAll(mapCounters);
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>>(mrSpec.getJobName()
        + "-reduce") {
      // Not really meant to be serialized, but avoid warning.
      private static final long serialVersionUID = 575338448598450119L;

      @Override
      public void failed(Status status) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void completed(
          List<? extends WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>>> tasks) {
        for (WorkerShardTask<KeyValue<K, Iterator<V>>, O, ReducerContext<O>> task : tasks) {
          counters.addAll(task.getContext().getCounters());
        }
      }
    });
    log.info("Reduce phase completed, reduce counters=" + counters);
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(outputs), counters);
  }

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

  private static String getMapReduceId() {
    DateTime dt = new DateTime();
    return "in-process-mr-" + DATE_FORMAT.print(dt) + "-" + new Random().nextInt(1000000);
  }

  public static <I, K, V, O, R> MapReduceResult<R> runMapReduce(
      MapReduceSpecification<I, K, V, O, R> mrSpec) throws IOException {
    String mapReduceId = getMapReduceId();
    InProcessMapReduce<I, K, V, O, R> mapReduce = new InProcessMapReduce<>(mapReduceId, mrSpec);
    log.info(mapReduce + " started");

    List<? extends InputReader<I>> mapInput = mrSpec.getInput().createReaders();
    InMemoryOutput<KeyValue<K, V>> mapOutput = InMemoryOutput.create(mapInput.size());
    MapReduceResult<List<List<KeyValue<K, V>>>> mapResult = mapReduce.map(mapInput, mapOutput);
    Output<O, R> reduceOutput = mrSpec.getOutput();
    List<List<KeyValue<K, List<V>>>> reducerInputs =
        mapReduce.shuffle(mapResult.getOutputResult(), reduceOutput.getNumShards());
    MapReduceResult<R> result =
        mapReduce.reduce(reducerInputs, reduceOutput, mapResult.getCounters());

    log.info(mapReduce + " finished");
    return result;
  }
}
