// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.mapreduce.impl.HashingSharder;
import com.google.appengine.tools.mapreduce.impl.InProcessMap;
import com.google.appengine.tools.mapreduce.impl.InProcessMapReduce;
import com.google.appengine.tools.mapreduce.impl.MapReduceConstants;
import com.google.appengine.tools.mapreduce.impl.shardedjob.ShardFailureException;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.inputs.ForwardingInputReader;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.mapreduce.inputs.RandomLongInput;
import com.google.appengine.tools.mapreduce.outputs.BlobFileOutput;
import com.google.appengine.tools.mapreduce.outputs.BlobFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.ForwardingOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutput;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.outputs.StringOutput;
import com.google.appengine.tools.mapreduce.reducers.KeyProjectionReducer;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.mapreduce.reducers.ValueProjectionReducer;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.easymock.EasyMock;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
@SuppressWarnings("deprecation")
public class EndToEndTest extends EndToEndTestCase {

  private static final Logger log = Logger.getLogger(EndToEndTest.class.getName());

  private PipelineService pipelineService;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    pipelineService = PipelineServiceFactory.newPipelineService();
  }

  private interface Verifier<R> {
    void verify(MapReduceResult<R> result) throws Exception;
  }

  private class NopVerifier<R> implements Verifier<R> {
    @Override
    public void verify(MapReduceResult<R> result) {
      // Do nothing
    }
  }

  private <I, K, V, O, R> void runWithPipeline(MapReduceSettings settings,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    String jobId = pipelineService.startNewPipeline(new MapReduceJob<>(mrSpec, settings));
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    @SuppressWarnings("unchecked")
    MapReduceResult<R> result = (MapReduceResult<R>) info.getOutput();
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, info.getJobState());
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, K, V, O, R> void runTest(
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    runTest(new MapReduceSettings.Builder().build(), mrSpec, verifier);
  }

  private <I, K, V, O, R> void runTest(MapReduceSettings settings,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    verifier.verify(InProcessMapReduce.runMapReduce(mrSpec));
    runWithPipeline(settings, mrSpec, verifier);
  }

  private <I, O, R> void runWithPipeline(MapSettings settings,
      MapSpecification<I, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    String jobId = pipelineService.startNewPipeline(new MapJob<>(mrSpec, settings));
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    @SuppressWarnings("unchecked")
    MapReduceResult<R> result = (MapReduceResult<R>) info.getOutput();
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, info.getJobState());
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, O, R> void runTest(MapSpecification<I, O, R> mrSpec, Verifier<R> verifier)
      throws Exception {
    runTest(mrSpec, new MapSettings.Builder().build(), verifier);
  }

  private <I, O, R> void runTest(MapSpecification<I, O, R> mrSpec, MapSettings settings,
      Verifier<R> verifier) throws Exception {
    verifier.verify(InProcessMap.runMap(mrSpec));
    runWithPipeline(settings, mrSpec, verifier);
  }

  public static class MapOnly extends MapOnlyMapper<Long, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public void map(Long value) {
      emit(String.valueOf(value));
    }
  }

  public void testMapOnlyJob() throws Exception {
      RandomLongInput input = new RandomLongInput(100, 2);
      InMemoryOutput<String> output = new InMemoryOutput<>();
      MapSpecification<Long, String, List<List<String>>> specification =
          new MapSpecification.Builder<>(input, new MapOnly(), output)
              .setJobName("mr-only")
              .build();
      runTest(specification, new Verifier<List<List<String>>>() {
        @Override
        public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
          Counters counters = result.getCounters();
          assertEquals(100, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
          assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
          List<List<String>> outputResult = result.getOutputResult();
          assertEquals(2, outputResult.size());
          assertEquals(50, outputResult.get(0).size());
          assertEquals(50, outputResult.get(1).size());
        }
      });
  }

  public static class VoidKeyMapper extends Mapper<Long, Void, String> {

    private static final long serialVersionUID = 1L;
    private final AtomicBoolean[] beginShard = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] endShard = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] inSlice = {new AtomicBoolean(), new AtomicBoolean()};
    private final AtomicBoolean[] memory = {new AtomicBoolean(), new AtomicBoolean()};

    @Override
    public void beginShard() {
      MapperContext<Void, String> ctx = getContext();
      assertNotNull(ctx.getJobId());
      assertEquals(2, ctx.getShardCount());
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertFalse(endShard[shard].get());
      beginShard[shard].set(true);
    }

    @Override
    public void beginSlice() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertFalse(endShard[shard].get());
      assertTrue(beginShard[shard].get());
      inSlice[shard].set(true);
    }

    @Override
    public void endSlice() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertTrue(inSlice[shard].get());
      assertTrue(beginShard[shard].get());
      assertFalse(endShard[shard].get());
      inSlice[shard].set(false);
    }

    @Override
    public void endShard() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertFalse(inSlice[shard].get());
      assertTrue(beginShard[ctx.getShardNumber()].get());
      assertFalse(endShard[shard].get());
      beginShard[shard].set(false);
      endShard[shard].set(true);
    }

    @Override
    public long estimateMemoryRequirement() {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      memory[shard].set(true);
      return 0;
    }

    @Override
    public void map(Long value) {
      MapperContext<Void, String> ctx = getContext();
      int shard = ctx.getShardNumber();
      assertTrue(inSlice[shard].get());
      assertTrue(beginShard[shard].get());
      assertFalse(endShard[shard].get());
      assertTrue(memory[shard].get());
      emit(null, String.valueOf(value));
      getContext().incrementCounter("my-counter");
    }
  }

  public void testAdaptedMapOnlyJob() throws Exception {
      RandomLongInput input = new RandomLongInput(100, 2);
      InMemoryOutput<String> output = new InMemoryOutput<>();
      MapOnlyMapper<Long, String> mapper = MapOnlyMapper.forMapper(new VoidKeyMapper());
      MapSpecification<Long, String, List<List<String>>> specification =
          new MapSpecification.Builder<>(input, mapper, output)
              .setJobName("adapted-mr-only")
              .build();
      runTest(specification, new Verifier<List<List<String>>>() {
        @Override
        public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
          Counters counters = result.getCounters();
          assertEquals(100, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
          assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
          assertEquals(100, counters.getCounter("my-counter").getValue());
          List<List<String>> outputResult = result.getOutputResult();
          assertEquals(2, outputResult.size());
          assertEquals(50, outputResult.get(0).size());
          assertEquals(50, outputResult.get(1).size());
        }
      });
  }

  public void testSomeShardsEmpty() throws Exception {
    runTest(new MapReduceSpecification.Builder<>(
        new ConsecutiveLongInput(0, 5, 10), new Mod37Mapper(),
        ValueProjectionReducer.<String, Long>create(), new InMemoryOutput<Long>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setJobName("Empty test MR")
        .setNumReducers(10)
        .build(), new Verifier<List<List<Long>>>() {
      @Override
      public void verify(MapReduceResult<List<List<Long>>> result) throws Exception {
        assertNotNull(result.getOutputResult());
        assertEquals(5, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(5, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
        HashSet<Long> allOutput = new HashSet<>();
        for (List<Long> output : result.getOutputResult()) {
          allOutput.addAll(output);
        }
        assertEquals(5, allOutput.size());
      }
    });
  }

  public void testManyReduceShards() throws Exception {
    runTest(new MapReduceSpecification.Builder<>(
        new ConsecutiveLongInput(0, 50000, 10), new Mod37Mapper(),
        ValueProjectionReducer.<String, Long>create(), new InMemoryOutput<Long>())
        .setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setJobName("Empty test MR")
        .setNumReducers(MapReduceConstants.MAX_WRITER_FANOUT * 2 + 1)
        .build(), new Verifier<List<List<Long>>>() {
      @Override
      public void verify(MapReduceResult<List<List<Long>>> result) throws Exception {
        assertNotNull(result.getOutputResult());
        assertEquals(50000, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
        assertEquals(37, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
        HashSet<Long> allOutput = new HashSet<>();
        for (List<Long> output : result.getOutputResult()) {
          allOutput.addAll(output);
        }
        assertEquals(50000, allOutput.size());
      }
    });
  }

  public void testDoNothingWithEmptyReadersList() throws Exception {
    runTest(
        new MapReduceSpecification.Builder<>(
            new NoInput<Long>(0),
            new Mod37Mapper(),
            NoReducer.<String, Long, String>create(),
            new NoOutput<String, String>())
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getLongMarshaller())
                .setJobName("Empty test MR")
                .build(),
        new Verifier<String>() {
          @Override
          public void verify(MapReduceResult<String> result) throws Exception {
            assertNull(result.getOutputResult());
            assertEquals(0, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
          }
        });
  }

  public void testDoNothing() throws Exception {
    runTest(
        new MapReduceSpecification.Builder<>(
            new NoInput<Long>(1),
            new Mod37Mapper(),
            NoReducer.<String, Long, String>create(),
            new NoOutput<String, String>())
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getLongMarshaller())
                .setJobName("Empty test MR")
                .build(),
        new Verifier<String>() {
          @Override
          public void verify(MapReduceResult<String> result) throws Exception {
            assertNull(result.getOutputResult());
            assertEquals(0, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, result.getCounters().getCounter(CounterNames.REDUCER_CALLS).getValue());
          }
        });
  }

  @SuppressWarnings("serial")
  private static class RougeMapper extends Mapper<Long, String, Long> {

    private static int[] beginShardCount;
    private static int[] endShardCount;
    private static int[] beginSliceCount;
    private static int[] endSliceCount;
    private static int[] totalMapCount;
    private static int[] successfulMapCount;
    private static Map<Integer, AtomicInteger>[] sliceAttemptsPerShardRetry;
    private static int[] shardFailureCount;
    private static int[] sliceFailureCount;
    private static boolean firstSlice; // fail at 2nd slice to avoid beginShard for slice failure
    private final int sliceFailures;
    private final int shardFailures;
    private int shardNum;
    private boolean allowSliceRetry = true;

    @SuppressWarnings("unchecked")
    private RougeMapper(int shards, int shardFailures, int sliceFailures) {
      this.shardFailures = shardFailures;
      this.sliceFailures = sliceFailures;
      beginShardCount = new int[shards];
      endShardCount = new int[shards];
      beginSliceCount = new int[shards];
      endSliceCount = new int[shards];
      totalMapCount = new int[shards];
      successfulMapCount = new int[shards];
      shardFailureCount = new int[shards];
      sliceFailureCount = new int[shards];
      sliceAttemptsPerShardRetry = new HashMap[shards];
      for (int i = 0; i < shards; i++) {
        sliceAttemptsPerShardRetry[i] = new HashMap<>();
      }
    }

    void setAllowSliceRetry(boolean allowSliceRetry) {
      this.allowSliceRetry = allowSliceRetry;
    }

    @Override
    public boolean allowSliceRetry() {
      return allowSliceRetry;
    }

    private void incrementCounter(String name) {
      getContext().incrementCounter(name + "[" + shardNum + "]");
    }

    private void incrementCounter(String name, int... indexes) {
      String counterName = name + "[" + shardNum + "]";
      for (int idx : indexes) {
        counterName += "[" + idx + "]";
      }
      getContext().incrementCounter(counterName);
    }

    @Override
    public void beginShard() {
      shardNum = getContext().getShardNumber();
      // We expect this to be one (as counter should be reset upon beginShard)
      incrementCounter("beginShard");
      sliceAttemptsPerShardRetry[shardNum].put(++beginShardCount[shardNum], new AtomicInteger());
      firstSlice = true;
    }

    @Override
    public void beginSlice() {
      shardNum = getContext().getShardNumber();
      sliceAttemptsPerShardRetry[shardNum].get(beginShardCount[shardNum]).getAndIncrement();
      incrementCounter("beginSlice", beginShardCount[shardNum]);
      beginSliceCount[shardNum]++;
    }

    @Override
    public void endSlice() {
      // We expect only endSlice[last_successful_attempt] to be available
      incrementCounter("endSlice", beginShardCount[shardNum]);
      endSliceCount[shardNum]++;
      firstSlice = false;
    }

    @Override
    public void endShard() {
      // We expect both to be equal to 1
      endShardCount[shardNum]++;
      incrementCounter("endShard");
    }

    @Override
    public void map(Long input) {
      totalMapCount[shardNum]++;
      incrementCounter("map");
      if (!firstSlice) {
        if (sliceFailureCount[shardNum] < sliceFailures) {
          sliceFailureCount[shardNum]++;
          throw new RuntimeException("bla");
        }
        if (shardFailureCount[shardNum] < shardFailures) {
          shardFailureCount[shardNum]++;
          throw new ShardFailureException("Bad state");
        }
      }
      successfulMapCount[shardNum]++;
    }
  }

  private static class RougeMapperVerifier implements Verifier<String> {

    private final int shards;
    private final int mapCount;
    private final int shardFailures;
    private final int sliceFailures;
    private final int shardFailuresDueToSliceRetry;
    private final int successfulMapCount; // we process one map call per slice
    private final int totalMapCount; // ShardFailure occur in the map
    private final int beginShardCount;
    private final int endShardCount;
    private final int beginSliceCount;
    private final int endSliceCount;
    private final Map<String, Integer> countersMap = new HashMap<>();

    private static final int SLICE_RETRIES = 20;
    private static final int SLICE_ATTEMPTS = 1 + SLICE_RETRIES;

    RougeMapperVerifier(int shards, int mapCount, int shardFailures, int sliceFailures) {
      this.shards = shards;
      this.mapCount = mapCount;
      this.sliceFailures = sliceFailures;
      this.shardFailuresDueToSliceRetry = sliceFailures / SLICE_ATTEMPTS;
      this.shardFailures = shardFailuresDueToSliceRetry + shardFailures;
      successfulMapCount = mapCount + this.shardFailures;
      totalMapCount = mapCount + 2 * shardFailures
          + (1 + SLICE_ATTEMPTS) * shardFailuresDueToSliceRetry + sliceFailures % SLICE_ATTEMPTS;
      beginShardCount = this.shardFailures + 1;
      endShardCount = 1; // always 1
      // 1 extra for eof (for both [begin/end]Slice)
      beginSliceCount =  1 + totalMapCount;
      endSliceCount = 1 + this.shardFailures + mapCount;

    }

    @Override
    public void verify(MapReduceResult<String> result) throws Exception {
      Counters counters = result.getCounters();
      assertEquals(shards * mapCount, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
      assertEquals(0, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
      countersMap.clear();
      for (Counter counter : counters.getCounters()) {
        countersMap.put(counter.getName(), (int) counter.getValue());
      }
      for (int i = 0; i < shards; i++){
        verify(i);
      }
    }

    private int getCounter(int shard, String name, int... indexes) {
      String counterName = name + "[" + shard + "]";
      for (int index : indexes) {
        counterName += "[" + index + "]";
      }
      return countersMap.containsKey(counterName) ? countersMap.get(counterName) : -1;
    }

    private void verify(int shard) {
      assertEquals(beginShardCount, RougeMapper.beginShardCount[shard]);
      assertEquals(endShardCount, RougeMapper.endShardCount[shard]);
      assertEquals(beginSliceCount, RougeMapper.beginSliceCount[shard]);
      assertEquals(endSliceCount, RougeMapper.endSliceCount[shard]);
      assertEquals(successfulMapCount, RougeMapper.successfulMapCount[shard]);
      assertEquals(totalMapCount, RougeMapper.totalMapCount[shard]);

      int shardTry = 1;
      while (shardTry <= shardFailuresDueToSliceRetry) {
         // 1 go through, 1 fail + 20 retries fail
        assertEquals(1 + SLICE_ATTEMPTS,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        shardTry++;
      }
      int failedSlices = sliceFailures % SLICE_ATTEMPTS;
      while (shardTry <= shardFailures) {
        if (failedSlices > 0) {
          assertEquals(1 + failedSlices + 1,
              RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
          failedSlices = 0;
        } else {
          assertEquals(2, RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        }
        shardTry++;
      }
      while (shardTry <= beginShardCount) {
        assertEquals(failedSlices + mapCount + 1,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(shardTry).intValue());
        failedSlices = 0;
        shardTry++;
      }

      for (int i = 1; i < beginShardCount; i++) {
        assertEquals(-1, getCounter(shard, "beginSlice", i));
        assertEquals(-1, getCounter(shard, "endSlice", i));
      }
      // our MR process 1 map per slice (and failures do not count)
      assertEquals(mapCount + 1, getCounter(shard, "beginSlice", beginShardCount));
      assertEquals(mapCount + 1, getCounter(shard, "endSlice", beginShardCount));
      // always 1 because counters are reset per shard
      assertEquals(1, getCounter(shard, "beginShard"));
      assertEquals(1, getCounter(shard, "endShard"));
      assertEquals(mapCount, getCounter(shard, "map"));
    }
  }

  public void testShardAndSliceRetriesSuccess() throws Exception {
    int shardsCount = 1;
    // {input-size, shard-failure, slice-failure}
    int[][] runs = {{10, 0, 0}, {10, 2, 0}, {10, 0, 10}, {10, 3, 5}, {10, 0, 22}, {10, 1, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(run[0], shardsCount);
      runWithPipeline(new MapReduceSettings.Builder().setMillisPerSlice(0).build(),
          new MapReduceSpecification.Builder<>(
              input,
              new RougeMapper(shardsCount, run[1], run[2]),
              NoReducer.<String, Long, String>create(),
              new NoOutput<String, String>())
                  .setKeyMarshaller(Marshallers.getStringMarshaller())
                  .setValueMarshaller(Marshallers.getLongMarshaller())
                  .setJobName("Shard-retry test")
                  .build(),
          new RougeMapperVerifier(shardsCount, run[0], run[1], run[2]) {
            @Override
            public void verify(MapReduceResult<String> result) throws Exception {
              super.verify(result);
              assertNull(result.getOutputResult());
            }
          });
    }

    // Disallow slice-retry
    runs = new int[][]{{10, 0, 0}, {10, 4, 0}, {10, 0, 4}, {10, 3, 1}, {10, 1, 3}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(run[0], shardsCount);
      RougeMapper mapper = new RougeMapper(shardsCount, run[1], run[2]);
      mapper.setAllowSliceRetry(false);
      runWithPipeline(new MapReduceSettings.Builder().setMillisPerSlice(0).build(),
          new MapReduceSpecification.Builder<>(
              input,
              mapper,
              NoReducer.<String, Long, String>create(),
              new NoOutput<String, String>())
                  .setKeyMarshaller(Marshallers.getStringMarshaller())
                  .setValueMarshaller(Marshallers.getLongMarshaller())
                  .setJobName("Shard-retry test")
                  .build(),
          new RougeMapperVerifier(shardsCount, run[0], run[1] + run[2], 0) {
            @Override
            public void verify(MapReduceResult<String> result) throws Exception {
              super.verify(result);
              assertNull(result.getOutputResult());
            }
          });
    }
  }

  public void testShardAndSliceRetriesFailure() throws Exception {
    int shardsCount = 1;
    // {shard-failure, slice-failure}
    int[][] runs = {{5, 0}, {4, 21}, {3, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(10, shardsCount);
      MapReduceSettings mrSettings = new MapReduceSettings.Builder().setMillisPerSlice(0).build();
      MapReduceSpecification<Long, String, Long, String, String> mrSpec =
          new MapReduceSpecification.Builder<>(input, new RougeMapper(shardsCount, run[0], run[1]),
          NoReducer.<String, Long, String>create(), new NoOutput<String, String>())
          .setJobName("Shard-retry failed").setKeyMarshaller(Marshallers.getStringMarshaller())
          .setValueMarshaller(Marshallers.getLongMarshaller()).build();
      String jobId = pipelineService.startNewPipeline(new MapReduceJob<>(mrSpec, mrSettings));
      assertFalse(jobId.isEmpty());
      executeTasksUntilEmpty("default");
      JobInfo info = pipelineService.getJobInfo(jobId);
      assertNull(info.getOutput());
      assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
      assertTrue(info.getException().getMessage().matches(
          "Stage map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    }

    // Disallow slice-retry
    runs = new int[][]{{5, 0}, {0, 5}, {4, 1}, {1, 4}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(10, shardsCount);
      RougeMapper mapper = new RougeMapper(shardsCount, run[0], run[1]);
      mapper.setAllowSliceRetry(false);
      MapReduceSettings mrSettings = new MapReduceSettings.Builder().setMillisPerSlice(0).build();
      MapReduceSpecification<Long, String, Long, String, String> mrSpec =
          new MapReduceSpecification.Builder<>(input, mapper,
          NoReducer.<String, Long, String>create(), new NoOutput<String, String>())
          .setJobName("Shard-retry failed").setKeyMarshaller(Marshallers.getStringMarshaller())
          .setValueMarshaller(Marshallers.getLongMarshaller()).build();
      String jobId = pipelineService.startNewPipeline(new MapReduceJob<>(mrSpec, mrSettings));
      assertFalse(jobId.isEmpty());
      executeTasksUntilEmpty("default");
      JobInfo info = pipelineService.getJobInfo(jobId);
      assertNull(info.getOutput());
      assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
      assertTrue(info.getException().getMessage().matches(
          "Stage map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    }
  }
  public void testPassThroughToString() throws Exception {
    final RandomLongInput input = new RandomLongInput(10, 1);
    input.setSeed(0L);
    runTest(
        new MapReduceSpecification.Builder<>(
            input,
            new Mod37Mapper(),
            ValueProjectionReducer.<String, Long>create(),
            new StringOutput<Long, List<AppEngineFile>>(
                ",", new BlobFileOutput("Foo-%02d", "testType")))
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getLongMarshaller())
                .setJobName("TestPassThroughToString")
                .build(),
        new Verifier<List<AppEngineFile>>() {
          @Override
          public void verify(MapReduceResult<List<AppEngineFile>> result) throws Exception {
            assertEquals(1, result.getOutputResult().size());
            assertEquals(10, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            AppEngineFile file = result.getOutputResult().get(0);
            FileReadChannel ch = FileServiceFactory.getFileService().openReadChannel(file, false);
            BufferedReader reader =
                new BufferedReader(Channels.newReader(ch, US_ASCII.newDecoder(), -1));
            String line = reader.readLine();
            List<String> strings = Arrays.asList(line.split(","));
            assertEquals(10, strings.size());
            input.setSeed(0L);
            InputReader<Long> source = input.createReaders().get(0);
            for (int i = 0; i < 10; i++) {
              assertTrue(strings.contains(source.next().toString()));
            }
          }
        });
  }

  public void testPassByteBufferToGcs() throws Exception {
    final RandomLongInput input = new RandomLongInput(10, 1);
    input.setSeed(0L);
    MapReduceSpecification.Builder<Long, ByteBuffer, ByteBuffer, ByteBuffer,
        GoogleCloudStorageFileSet> builder = new MapReduceSpecification.Builder<>();
    builder.setJobName("TestPassThroughToByteBuffer");
    builder.setInput(input);
    builder.setMapper(new LongToBytesMapper());
    builder.setKeyMarshaller(Marshallers.getByteBufferMarshaller());
    builder.setValueMarshaller(Marshallers.getByteBufferMarshaller());
    builder.setReducer(ValueProjectionReducer.<ByteBuffer, ByteBuffer>create());
    builder.setOutput(new GoogleCloudStorageFileOutput(
        "bucket", "fileNamePattern-%04d", "application/octet-stream"));
    builder.setNumReducers(2);
    runTest(builder.build(),
        new Verifier<GoogleCloudStorageFileSet>() {
          @Override
          public void verify(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
            assertEquals(2, result.getOutputResult().getNumFiles());
            assertEquals(10, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            ArrayList<Long> results = new ArrayList<>();
            GcsService gcsService = GcsServiceFactory.createGcsService();
            ByteBuffer holder = ByteBuffer.allocate(8);
            for (GcsFilename file : result.getOutputResult().getFiles()) {
              try (GcsInputChannel channel = gcsService.openPrefetchingReadChannel(file, 0, 4096)) {
                int read = channel.read(holder);
                while (read != -1) {
                  holder.rewind();
                  results.add(holder.getLong());
                  holder.rewind();
                  read = channel.read(holder);
                }
              }
            }
            assertEquals(10, results.size());
            RandomLongInput input = new RandomLongInput(10, 1);
            input.setSeed(0L);
            InputReader<Long> source = input.createReaders().get(0);
            for (int i = 0; i < results.size(); i++) {
              Long expected = source.next();
              assertTrue(results.contains(expected));
            }
          }
        });
  }

  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestInputReader<T> extends ForwardingInputReader<T> {

    static InputReader delegate;

    public TestInputReader(InputReader<T> delegate) {
      TestInputReader.delegate = delegate;
    }

    @Override
    protected InputReader<T> getDelegate() {
      return delegate;
    }
  }

  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestInput<T> extends Input<T> {

    static Input delegate;

    public TestInput(Input<T> delegate) {
      TestInput.delegate = delegate;
    }

    @Override
    void setContext(Context context) {
      delegate.setContext(context);
    }

    @Override
    public List<? extends InputReader<T>> createReaders() throws IOException {
      return delegate.createReaders();
    }
  }

  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestOutputWriter<T> extends ForwardingOutputWriter<T> {

    static OutputWriter delegate;

    public TestOutputWriter(OutputWriter<T> delegate) {
      TestOutputWriter.delegate = delegate;
    }

    @Override
    protected OutputWriter<T> getDelegate() {
      return delegate;
    }

    @Override
    public void write(T value) throws IOException {
      delegate.write(value);
    }
  }

  @SuppressWarnings({"serial", "rawtypes", "unchecked"})
  static class TestOutput<O, R> extends Output<O, R> {

    static Output delegate;

    public TestOutput(Output<O, R> delegate) {
      TestOutput.delegate = delegate;
    }

    @Override
    public void setContext(Context context) {
      delegate.setContext(context);
    }

    @Override
    public List<? extends OutputWriter<O>> createWriters(int numShards) {
      return delegate.createWriters(numShards);
    }

    @Override
    public R finish(Collection<? extends OutputWriter<O>> writers) throws IOException {
      return (R) delegate.finish(writers);
    }
  }

  @SuppressWarnings("unchecked")
  public void testLifeCycleMethodsCalled() throws Exception {
    Input<Long> input = createStrictMock(Input.class);
    InputReader<Long> inputReader = createStrictMock(InputReader.class);
    Output<ByteBuffer, Void> output = createStrictMock(Output.class);
    OutputWriter<ByteBuffer> outputWriter = createStrictMock(OutputWriter.class);

    input.setContext(anyObject(Context.class));
    EasyMock.<List<? extends InputReader<Long>>>expect(input.createReaders())
        .andReturn(ImmutableList.of(new TestInputReader<>(inputReader)));
    inputReader.setContext(anyObject(ShardContext.class));
    expectLastCall().atLeastOnce();
    expect(inputReader.estimateMemoryRequirement()).andReturn(0L).atLeastOnce();
    inputReader.beginShard();
    inputReader.beginSlice();
    expect(inputReader.next()).andThrow(new NoSuchElementException());
    inputReader.endSlice();
    inputReader.endShard();
    inputReader.setContext(anyObject(ShardContext.class));
    expectLastCall().anyTimes();

    output.setContext(anyObject(Context.class));
    EasyMock.<List<? extends OutputWriter<ByteBuffer>>>expect(output.createWriters(1))
        .andReturn(ImmutableList.of(new TestOutputWriter<>(outputWriter)));
    outputWriter.setContext(anyObject(ShardContext.class));
    expectLastCall().atLeastOnce();
    expect(outputWriter.estimateMemoryRequirement()).andReturn(0L).atLeastOnce();
    outputWriter.beginShard();
    outputWriter.beginSlice();
    outputWriter.endSlice();
    outputWriter.endShard();
    outputWriter.setContext(anyObject(ShardContext.class));
    expectLastCall().anyTimes();

    output.setContext(anyObject(Context.class));
    expect(output.finish(isA(Collection.class))).andReturn(null);
    replay(input, inputReader, output, outputWriter);
    runWithPipeline(new MapReduceSettings.Builder().build(),
        new MapReduceSpecification.Builder<>(
            new TestInput<>(input),
            new LongToBytesMapper(),
            ValueProjectionReducer.<ByteBuffer, ByteBuffer>create(),
            new TestOutput<>(output))
                .setKeyMarshaller(Marshallers.getByteBufferMarshaller())
                .setValueMarshaller(Marshallers.getByteBufferMarshaller())
                .setJobName("testLifeCycleMethodsCalled")
                .build(),
        new NopVerifier<Void>());
    verify(input, inputReader, output, outputWriter);
  }

  public void testDatastoreData() throws Exception {
    final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    // Datastore restriction: id cannot be zero.
    for (long i = 1; i <= 100; ++i) {
      datastoreService.put(new Entity(KeyFactory.createKey("Test", i)));
    }
    runTest(
        new MapReduceSpecification.Builder<>(
            new DatastoreInput("Test", 5),
            new TestMapper(),
            new TestReducer(),
            new InMemoryOutput<KeyValue<String, List<Long>>>())
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getLongMarshaller())
                .setJobName("Test MR")
                .build(),
        new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
          @Override
          public void verify(MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
              throws Exception {
            Counters counters = result.getCounters();
            log.info("counters=" + counters);
            assertNotNull(counters);

            assertEquals(100, counters.getCounter("map").getValue());
            assertEquals(5, counters.getCounter("beginShard").getValue());
            assertEquals(5, counters.getCounter("endShard").getValue());
            assertEquals(5, counters.getCounter("beginSlice").getValue());
            assertEquals(5, counters.getCounter("endSlice").getValue());

            assertEquals(100, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertTrue(counters.getCounter(CounterNames.MAPPER_WALLTIME_MILLIS).getValue() > 0);

            Query query = new Query("Test");
            for (Entity e : datastoreService.prepare(query).asIterable()) {
              Object mark = e.getProperty("mark");
              assertNotNull(mark);
            }

            List<KeyValue<String, List<Long>>> output =
                Iterables.getOnlyElement(result.getOutputResult());
            assertEquals(2, output.size());
            assertEquals("even", output.get(0).getKey());
            List<Long> evenValues = new ArrayList<>(output.get(0).getValue());
            Collections.sort(evenValues);
            assertEquals(ImmutableList.of(
                        2L, 4L, 6L, 8L,
                        10L, 12L, 14L, 16L, 18L,
                        20L, 22L, 24L, 26L, 28L,
                        30L, 32L, 34L, 36L, 38L,
                        40L, 42L, 44L, 46L, 48L,
                        50L, 52L, 54L, 56L, 58L,
                        60L, 62L, 64L, 66L, 68L,
                        70L, 72L, 74L, 76L, 78L,
                        80L, 82L, 84L, 86L, 88L,
                        90L, 92L, 94L, 96L, 98L,
                        100L), evenValues);
            assertEquals("multiple-of-ten", output.get(1).getKey());
            List<Long> multiplesOfTen = new ArrayList<>(output.get(1).getValue());
            Collections.sort(multiplesOfTen);
            assertEquals(ImmutableList.of(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L),
                multiplesOfTen);
          }
        });
  }

  public void testNoData() throws Exception {
    runTest(
        new MapReduceSpecification.Builder<>(
            new DatastoreInput("Test", 2),
            new TestMapper(),
            NoReducer.<String, Long, Void>create(),
            new NoOutput<Void, Void>())
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getLongMarshaller())
                .setJobName("Test MR")
                .build(),
        new Verifier<Void>() {
          @Override
          public void verify(MapReduceResult<Void> result) throws Exception {
            Counters counters = result.getCounters();
            assertNotNull(counters);

            assertEquals(0, counters.getCounter("map").getValue());
            assertEquals(0, counters.getCounter("beginShard").getValue());
            assertEquals(0, counters.getCounter("endShard").getValue());
            assertEquals(0, counters.getCounter("beginSlice").getValue());
            assertEquals(0, counters.getCounter("endSlice").getValue());

            assertEquals(0, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(0, counters.getCounter(CounterNames.MAPPER_WALLTIME_MILLIS).getValue());
          }
        });
  }

  @SuppressWarnings("serial")
  private static class Mod37Mapper extends Mapper<Long, String, Long> {

    @Override
    public void map(Long input) {
      String mod37 = String.valueOf(Math.abs(input) % 37);
      emit(mod37, input);
    }
  }

  @SuppressWarnings("serial")
  private static class DummyValueMapper extends Mapper<Long, Long, String> {
    private final String value;
    DummyValueMapper (int size) {
      StringBuilder b = new StringBuilder();
      for (int i=0;i<size;i++) {
        b.append('\0');
      }
      value = b.toString();
    }
    @Override
    public void map(Long input) {
      emit(input, value);
    }
  }

  @SuppressWarnings("serial")
  private static class LongToBytesMapper extends Mapper<Long, ByteBuffer, ByteBuffer> {

    @Override
    public void map(Long input) {
      ByteBuffer key = ByteBuffer.allocate(8);
      key.putLong(input);
      key.rewind();
      ByteBuffer value = ByteBuffer.allocate(8);
      value.putLong(input);
      value.rewind();
      emit(key, value);
    }
  }

  public void testSomeNumbers() throws Exception {
    MapReduceSpecification.Builder<Long, String, Long, KeyValue<String, List<Long>>,
        List<List<KeyValue<String, List<Long>>>>> mrSpecBuilder =
            new MapReduceSpecification.Builder<>();
    mrSpecBuilder.setJobName("Test MR");
    mrSpecBuilder.setInput(new ConsecutiveLongInput(-10000, 10000, 10));
    mrSpecBuilder.setMapper(new Mod37Mapper());
    mrSpecBuilder.setKeyMarshaller(Marshallers.getStringMarshaller());
    mrSpecBuilder.setValueMarshaller(Marshallers.getLongMarshaller());
    mrSpecBuilder.setReducer(new TestReducer());
    mrSpecBuilder.setOutput(new InMemoryOutput<KeyValue<String, List<Long>>>());
    mrSpecBuilder.setNumReducers(5);

    runTest(mrSpecBuilder.build(),
        new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
          @Override
          public void verify(MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
              throws Exception {
            Counters counters = result.getCounters();
            assertEquals(20000, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(37, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

            List<List<KeyValue<String, List<Long>>>> actualOutput = result.getOutputResult();
            List<ArrayListMultimap<String, Long>> expectedOutput = Lists.newArrayList();
            for (int i = 0; i < 5; i++) {
              expectedOutput.add(ArrayListMultimap.<String, Long>create());
            }
            Marshaller<String> marshaller = Marshallers.getStringMarshaller();
            HashingSharder sharder = new HashingSharder(5);
            for (long l = -10000; l < 10000; l++) {
              String mod37 = String.valueOf(Math.abs(l) % 37);
              expectedOutput.get(sharder.getShardForKey(marshaller.toBytes(mod37)))
                  .put(mod37, l);
            }
            for (int i = 0; i < 5; i++) {
              assertEquals(expectedOutput.get(i).keySet().size(), actualOutput.get(i).size());
              for (KeyValue<String, List<Long>> actual : actualOutput.get(i)) {
                List<Long> value = new ArrayList<>(actual.getValue());
                Collections.sort(value);
                assertEquals("shard " + i + ", key " + actual.getKey(),
                    expectedOutput.get(i).get(actual.getKey()), value);
              }
            }
          }
        });
  }

  /**
   * Makes sure the same key is not dupped, nor does the reduce go into an infinite loop if it
   * ignores the values.
   */
  public void testReduceOnlyLooksAtKeys() throws Exception {
    MapReduceSpecification.Builder<Long, String, Long, String, List<List<String>>>  builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    builder.setInput(new ConsecutiveLongInput(-10000, 10000, 10));
    builder.setMapper(new Mod37Mapper());
    builder.setKeyMarshaller(Marshallers.getStringMarshaller());
    builder.setValueMarshaller(Marshallers.getLongMarshaller());
    builder.setReducer(KeyProjectionReducer.<String, Long>create());
    builder.setOutput(new InMemoryOutput<String>());
    builder.setNumReducers(5);

    runTest(builder.build(),
       new Verifier<List<List<String>>>() {
          @Override
          public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
            Counters counters = result.getCounters();
            assertEquals(20000, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(37, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

            List<List<String>> actualOutput = result.getOutputResult();
            assertEquals(5, actualOutput.size());
            List<String> allKeys = new ArrayList<>();
            for (int shard = 0; shard < 5; shard++) {
              allKeys.addAll(actualOutput.get(shard));
            }
            assertEquals(37, allKeys.size());
            for (int i = 0; i < 37; i++) {
              assertTrue(String.valueOf(i), allKeys.contains(String.valueOf(i)));
            }
          }
        });
  }

  public void testManySortOutputFiles() throws Exception {
    MapReduceSpecification.Builder<Long, Long, String, Long, List<List<Long>>> builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    final long sortMem = 1000;
    final long inputItems = 5 * MapReduceConstants.MERGE_FANIN * sortMem / 100;
    builder.setInput(new ConsecutiveLongInput(0, inputItems, 1));
    builder.setMapper(new DummyValueMapper(100));
    builder.setKeyMarshaller(Marshallers.getLongMarshaller());
    builder.setValueMarshaller(Marshallers.getStringMarshaller());
    builder.setReducer(KeyProjectionReducer.<Long, String>create());
    builder.setOutput(new InMemoryOutput<Long>());
    builder.setNumReducers(1);
    runWithPipeline(new MapReduceSettings.Builder().setMaxSortMemory(sortMem).build(),
        builder.build(), new Verifier<List<List<Long>>>() {
          @Override
          public void verify(MapReduceResult<List<List<Long>>> result) throws Exception {
            Counters counters = result.getCounters();
            assertEquals(inputItems, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(inputItems, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());
            assertEquals(inputItems, counters.getCounter(CounterNames.SORT_CALLS).getValue());
            assertEquals(inputItems, counters.getCounter(CounterNames.MERGE_CALLS).getValue());

            List<List<Long>> actualOutput = result.getOutputResult();
            assertEquals(1, actualOutput.size());
            assertEquals(inputItems, actualOutput.get(0).size());
          }
        });
  }

  public void testSlicingJob() throws Exception {
    MapReduceSpecification.Builder<Long, String, Long, String, List<List<String>>> builder =
        new MapReduceSpecification.Builder<>();
    builder.setJobName("Test MR");
    builder.setInput(new ConsecutiveLongInput(-100, 100, 10));
    builder.setMapper(new Mod37Mapper());
    builder.setKeyMarshaller(Marshallers.getStringMarshaller());
    builder.setValueMarshaller(Marshallers.getLongMarshaller());
    builder.setReducer(KeyProjectionReducer.<String, Long>create());
    builder.setOutput(new InMemoryOutput<String>());
    builder.setNumReducers(5);
    runTest(new MapReduceSettings.Builder().setMillisPerSlice(0).build(), builder.build(),
        new Verifier<List<List<String>>>() {
          @Override
          public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
            Counters counters = result.getCounters();
            assertEquals(200, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(37, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

            List<List<String>> actualOutput = result.getOutputResult();
            assertEquals(5, actualOutput.size());
            List<String> allKeys = new ArrayList<>();
            for (int shard = 0; shard < 5; shard++) {
              allKeys.addAll(actualOutput.get(shard));
            }
            assertEquals(37, allKeys.size());
            for (int i = 0; i < 37; i++) {
              assertTrue(String.valueOf(i), allKeys.contains(String.valueOf(i)));
            }
          }
        });
  }

  @SuppressWarnings("serial")
  static class SideOutputMapper extends Mapper<Long, String, Void> {
    transient BlobFileOutputWriter sideOutput;

    @Override
    public void beginSlice() {
      sideOutput = new BlobFileOutputWriter("test file", "application/octet-stream");
      try {
        sideOutput.beginShard();
        sideOutput.beginSlice();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void map(Long input) {
      log.info("map(" + input + ") in shard " + getContext().getShardNumber());
      try {
        sideOutput.write(Marshallers.getLongMarshaller().toBytes(input));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void endSlice() {
      log.info("endShard() in shard " + getContext().getShardNumber());
      try {
        sideOutput.endSlice();
        sideOutput.endShard();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      emit(sideOutput.getFile().getFullPath(), null);
    }
  }

  public void testSideOutput() throws Exception {

    runTest(
        new MapReduceSpecification.Builder<>(
            new ConsecutiveLongInput(0, 6, 6),
            new SideOutputMapper(),
            KeyProjectionReducer.<String, Void>create(),
            new InMemoryOutput<String>())
                .setKeyMarshaller(Marshallers.getStringMarshaller())
                .setValueMarshaller(Marshallers.getVoidMarshaller())
                .setJobName("Test MR")
                .build(),
        new Verifier<List<List<String>>>() {
          @Override
          public void verify(MapReduceResult<List<List<String>>> result) throws Exception {
            List<List<String>> outputResult = result.getOutputResult();
            Set<Long> expected = new HashSet<>();
            for (long i = 0; i < 6; i++) {
              expected.add(i);
            }
            assertEquals(1, outputResult.size());
            for (List<String> files : outputResult) {
              assertEquals(6, files.size());
              for (String file : files) {
                ByteBuffer buf = ByteBuffer.allocate(8);
                try (FileReadChannel ch = FileServiceFactory.getFileService().openReadChannel(
                    new AppEngineFile(file), false)) {
                  assertEquals(8, ch.read(buf));
                  assertEquals(-1, ch.read(ByteBuffer.allocate(1)));
                }
                buf.flip();
                assertTrue(expected.remove(Marshallers.getLongMarshaller().fromBytes(buf)));
              }
            }
            assertTrue(expected.isEmpty());
          }
        });
  }

  @SuppressWarnings("serial")
  static class TestMapper extends Mapper<Entity, String, Long> {

    transient DatastoreMutationPool pool;

    @Override
    public void map(Entity entity) {
      getContext().incrementCounter("map");
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      long key = entity.getKey().getId();
      log.info("map(" + key + ")");
      if (key % 2 == 0) {
        emit("even", key);
      }
      if (key % 10 == 0) {
        emit("multiple-of-ten", key);
      }
      entity.setProperty("mark", Boolean.TRUE);
      pool.put(entity);
    }

    @Override
    public void beginShard() {
      getContext().incrementCounter("beginShard");
    }

    @Override
    public void endShard() {
      getContext().incrementCounter("endShard");
    }

    @Override
    public void beginSlice() {
      pool = DatastoreMutationPool.create();
      getContext().incrementCounter("beginSlice");
    }

    @Override
    public void endSlice() {
      pool.flush();
      getContext().incrementCounter("endSlice");
    }
  }

  @SuppressWarnings("serial")
  static class TestReducer extends Reducer<String, Long, KeyValue<String, List<Long>>> {
    @Override
    public void reduce(String property, ReducerInput<Long> matchingValues) {
      ImmutableList.Builder<Long> out = ImmutableList.builder();
      while (matchingValues.hasNext()) {
        long value = matchingValues.next();
        getContext().incrementCounter(property);
        out.add(value);
      }
      List<Long> values = out.build();
      emit(KeyValue.of(property, values));
    }
  }
}
