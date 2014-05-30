// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.mapreduce.Counters;
import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.google.appengine.tools.mapreduce.MapOnlyMapperContext;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.impl.shardedjob.InProcessShardedJobRunner;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardedJobController;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.mapreduce.impl.util.SerializationUtil;
import com.google.common.collect.ImmutableList;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Runs a Map only in the current process. Only for very small datasets. Easier to debug than a
 * parallel MapReduce.
 *
 * @param <I> type of input values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@code Output}
 */
public class InProcessMap<I, O, R> {

  private static final Logger log = Logger.getLogger(InProcessMap.class.getName());

  private final String id;
  private final String jobName;
  private final Input<I> input;
  private final MapOnlyMapper<I, O> mapper;
  private final Output<O, R> output;

  public InProcessMap(String id, MapSpecification<I, O, R> mapSpec) {
    this.id = checkNotNull(id, "Null id");
    jobName = InProcessUtil.getJobName(mapSpec) + "-map";
    input = InProcessUtil.getInput(mapSpec);
    mapper = InProcessUtil.getMapper(mapSpec);
    output = InProcessUtil.getOutput(mapSpec);
  }

  @Override
  public String toString() {
    return "InProcessMapOnly.Impl(" + id + ")";
  }

  private MapReduceResultImpl<R> map() throws IOException {
    log.info("Map started");
    List<? extends InputReader<I>> readers = input.createReaders();
    List<? extends OutputWriter<O>> writers = output.createWriters(readers.size());
    ImmutableList.Builder<WorkerShardTask<I, O , MapOnlyMapperContext<O>>> tasks =
        ImmutableList.builder();
    for (int shard = 0; shard < readers.size(); shard++) {
      WorkerShardTask<I, O, MapOnlyMapperContext<O>> task = new MapOnlyShardTask<>(
          id, shard, readers.size(), readers.get(shard), getCopyOfMapper(), writers.get(shard),
          Long.MAX_VALUE);
      tasks.add(task);
    }
    final Counters counters = new CountersImpl();
    InProcessShardedJobRunner.runJob(tasks.build(), new ShardedJobController<
        WorkerShardTask<I, O, MapOnlyMapperContext<O>>>(jobName) {
          // Not really meant to be serialized, but avoid warning.
          private static final long serialVersionUID = 661198005749484951L;

          @Override
          public void failed(Status status) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void completed(Iterator<WorkerShardTask<I, O, MapOnlyMapperContext<O>>> tasks) {
            while (tasks.hasNext()) {
              counters.addAll(tasks.next().getContext().getCounters());
            }
          }
        });
    log.info("Map completed");
    log.info("combined counters=" + counters);
    return new MapReduceResultImpl<>(output.finish(writers), counters);
  }


  @SuppressWarnings("unchecked")
  private MapOnlyMapper<I, O> getCopyOfMapper() {
    byte[] bytes = SerializationUtil.serializeToByteArray(mapper);
    return (MapOnlyMapper<I, O>) SerializationUtil.deserializeFromByteArray(bytes);
  }

  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

  private static String getMapReduceId() {
    DateTime dt = new DateTime();
    return "in-process-map-" + DATE_FORMAT.print(dt) + "-" + new Random().nextInt(1000000);
  }

  public static <I, O, R> MapReduceResult<R> runMap(MapSpecification<I, O, R> mrSpec)
      throws IOException {
    String mapReduceId = getMapReduceId();
    InProcessMap<I,  O, R> mapOnly = new InProcessMap<>(mapReduceId, mrSpec);
    log.info(mapOnly + " started");
    MapReduceResult<R> result = mapOnly.map();
    log.info(mapOnly + " finished");
    return result;
  }
}
