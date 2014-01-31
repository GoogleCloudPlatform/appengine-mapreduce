// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.replay;

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
import com.google.appengine.tools.mapreduce.impl.InProcessMapReduce;
import com.google.appengine.tools.mapreduce.impl.RecoverableException;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.mapreduce.inputs.RandomLongInput;
import com.google.appengine.tools.mapreduce.outputs.BlobFileOutput;
import com.google.appengine.tools.mapreduce.outputs.BlobFileOutputWriter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * @author ohler@google.com (Christian Ohler)
 */
public class EndToEndTest extends EndToEndTestCase {

  private static final Logger log = Logger.getLogger(EndToEndTest.class.getName());

  private PipelineService pipelineService;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    pipelineService = PipelineServiceFactory.newPipelineService();
  }

  private static class Preparer {
    MapReduceSettings prepare() {
      return new MapReduceSettings();
    }
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

  // (runWithPipeline is also in-process in our test setup, so this is a misnomer.)
  private <I, K, V, O, R> void runInProcess(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    preparer.prepare();
    verifier.verify(InProcessMapReduce.runMapReduce(mrSpec));
  }

  private <I, K, V, O, R> void runWithPipeline(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    MapReduceSettings mrSettings = preparer.prepare();
    String jobId = pipelineService.startNewPipeline(new MapReduceJob<I, K, V, O, R>(),
        mrSpec, mrSettings);
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    MapReduceResult<R> result = (MapReduceResult<R>) info.getOutput();
    assertEquals(JobInfo.State.COMPLETED_SUCCESSFULLY, info.getJobState());
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, K, V, O, R> void runTest(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    runInProcess(preparer, mrSpec, verifier);
    runWithPipeline(preparer, mrSpec, verifier);
  }

  public void testDoNothingWithEmptyReadersList() throws Exception {
    runTest(new Preparer(),
        MapReduceSpecification.of("Empty test MR",
            new NoInput<Long>(0),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, String>create(),
            new NoOutput<String, String>(1)),
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
    runTest(new Preparer(),
        MapReduceSpecification.of("Empty test MR",
            new NoInput<Long>(1),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, String>create(),
            new NoOutput<String, String>(1)),
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
    private static int[] sliceFailureCount;
    private static boolean firstSlice;
    private final int sliceFailures;
    private final int shardFailures;
    private int shardNum;

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
      sliceFailureCount = new int[shards];
      sliceAttemptsPerShardRetry = new HashMap[shards];
      for (int i = 0; i < shards; i++) {
        sliceAttemptsPerShardRetry[i] = new HashMap<>();
      }
    }

    @Override
    public void setContext(MapperContext<String, Long> context) {
      super.setContext(context);
      if (context != null) {
        shardNum = context.getShardNumber();
      }
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
      // We expect this to be one (as counter should be reset upon beginShard)
      incrementCounter("beginShard");
      sliceAttemptsPerShardRetry[shardNum].put(++beginShardCount[shardNum], new AtomicInteger());
      firstSlice = true;
    }

    @Override
    public void beginSlice() {
      sliceAttemptsPerShardRetry[shardNum].get(beginShardCount[shardNum]).getAndIncrement();
      // We expect only beginSlice[last_successful_attempt] to be available

      incrementCounter("beginSlice", beginShardCount[shardNum]);
      // Start with slice failures after, shard failure and a first successful slice
      beginSliceCount[shardNum]++;
      if (!firstSlice) {
        if (++sliceFailureCount[shardNum] <= sliceFailures) {
          throw new RecoverableException("bla", null);
        }
      }
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
      if (beginShardCount[shardNum] <= shardFailures) {
        throw new RuntimeException("Bad state");
      }
      // TODO(user): Once we support slice failure during map phase
      // by checking with the writer, we should have a test for it here.
      successfulMapCount[shardNum]++;
    }
  }

  private static class RougeMapperPreparer extends Preparer {

    public RougeMapperPreparer(RandomLongInput input) {
      input.setSeed(0L);
    }

    @Override
    public MapReduceSettings prepare() {
      MapReduceSettings settings = super.prepare();
      settings.setMillisPerSlice(0);
      return settings;
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
      this.shardFailures = shardFailures;
      this.sliceFailures = sliceFailures;
      shardFailuresDueToSliceRetry = sliceFailures / SLICE_ATTEMPTS;
      successfulMapCount = mapCount + shardFailuresDueToSliceRetry;
      totalMapCount = mapCount + shardFailures + shardFailuresDueToSliceRetry;
      beginShardCount = shardFailures + shardFailuresDueToSliceRetry + 1;
      endShardCount = 1; // always 1
      // 1 extra for eof (for both [begin/end]Slice)
      beginSliceCount = shardFailures + shardFailuresDueToSliceRetry + sliceFailures + mapCount + 1;
      endSliceCount = mapCount + shardFailuresDueToSliceRetry + 1;
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

      // check counter for shard failures
      for (int i = 1; i < shardFailures; i++) {
        assertEquals(1, RougeMapper.sliceAttemptsPerShardRetry[shard].get(i).intValue());
      }
      for (int i = shardFailures + 1; i < beginShardCount; i++) {
        // 1 go through, 1 fail + 20 retries fail
        assertEquals(1 + SLICE_ATTEMPTS,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(i).intValue());
      }
      if (shardFailuresDueToSliceRetry > 0){
        assertEquals(mapCount + 1 + sliceFailures % SLICE_ATTEMPTS,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(beginShardCount).intValue());
      } else {
        assertEquals(successfulMapCount + sliceFailures + 1,
            RougeMapper.sliceAttemptsPerShardRetry[shard].get(beginShardCount).intValue());
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

  public void testShardRetriesSuccess() throws Exception {
    int shardsCount = 1;
    // {input-size, shard-failure, slice-failure}
    int[][] runs = {{10, 2, 0}, {10, 0, 10}, {10, 3, 5}, {10, 0, 22}, {10, 1, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(run[0], shardsCount);
      runWithPipeline(new RougeMapperPreparer(input),
          MapReduceSpecification.of("Shard-retry test",
              input,
              new RougeMapper(shardsCount, run[1], run[2]),
              Marshallers.getStringMarshaller(),
              Marshallers.getLongMarshaller(),
              NoReducer.<String, Long, String>create(),
              new NoOutput<String, String>(1)),
          new RougeMapperVerifier(shardsCount, run[0], run[1], run[2]) {
            @Override
            public void verify(MapReduceResult<String> result) throws Exception {
              super.verify(result);
              assertNull(result.getOutputResult());
            }
          });
    }
  }

  public void testShardRetriesFailure() throws Exception {
    int shardsCount = 1;
    // {shard-failure, slice-failure}
    int[][] runs = {{5, 0}, {4, 21}, {3, 50}};
    for (int[] run : runs) {
      RandomLongInput input = new RandomLongInput(10, shardsCount);
      MapReduceSettings mrSettings = new RougeMapperPreparer(input).prepare();
      MapReduceSpecification<Long, String, Long, String, String> mrSpec =
          MapReduceSpecification.of(
              "Shard-retry failed", input, new RougeMapper(shardsCount, run[0], run[1]),
          Marshallers.getStringMarshaller(), Marshallers.getLongMarshaller(),
          NoReducer.<String, Long, String>create(), new NoOutput<String, String>(1));
      String jobId = pipelineService.startNewPipeline(
          new MapReduceJob<Long, String, Long, String, String>(), mrSpec, mrSettings);
      assertFalse(jobId.isEmpty());
      executeTasksUntilEmpty("default");
      JobInfo info = pipelineService.getJobInfo(jobId);
      assertNull(info.getOutput());
      assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
      assertTrue(info.getException().getMessage().matches(
          "Stage map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    }
  }

  // Slice exceeds max allowed and fallback to shard-retry
  public void testSliceRetriesFailure() throws Exception {
    int shardsCount = 1;
    final RandomLongInput input = new RandomLongInput(10, shardsCount);
    input.setSeed(0L);
    MapReduceSpecification<Long, String, Long, String, String> mrSpec =
        MapReduceSpecification.of("Shard-retry failed", input, new RougeMapper(shardsCount, 5, 0),
        Marshallers.getStringMarshaller(), Marshallers.getLongMarshaller(),
        NoReducer.<String, Long, String>create(), new NoOutput<String, String>(1));
    String jobId = pipelineService.startNewPipeline(
        new MapReduceJob<Long, String, Long, String, String>(), mrSpec, new MapReduceSettings());
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    assertNull(info.getOutput());
    assertEquals(JobInfo.State.STOPPED_BY_ERROR, info.getJobState());
    assertTrue(info.getException().getMessage().matches(
        "Stage map-.* was not completed successfuly \\(status=ERROR, message=.*\\)"));
    assertEquals("Shard 0 failed.", info.getException().getCause().getMessage());
    assertEquals("Bad state", info.getException().getCause().getCause().getMessage());
  }

  @SuppressWarnings("deprecation")
  public void testPassThroughToString() throws Exception {
    final RandomLongInput input = new RandomLongInput(10, 1);
    input.setSeed(0L);
    runTest(new Preparer(),
        MapReduceSpecification.of("TestPassThroughToString",
            input,
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            ValueProjectionReducer.<String, Long>create(),
            new StringOutput<Long, List<AppEngineFile>>(
                ",", new BlobFileOutput("Foo-%02d", "testType", 1))),
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
    runTest(new Preparer(),
        MapReduceSpecification.of("TestPassThroughToByteBuffer",
            input,
            new LongToBytesMapper(),
            Marshallers.getByteBufferMarshaller(),
            Marshallers.getByteBufferMarshaller(),
            ValueProjectionReducer.<ByteBuffer, ByteBuffer>create(),
            new GoogleCloudStorageFileOutput("bucket", "fileNamePattern-%04d",
                "application/octet-stream", 2)),
        new Verifier<GoogleCloudStorageFileSet>() {
          @Override
          public void verify(MapReduceResult<GoogleCloudStorageFileSet> result) throws Exception {
            assertEquals(2, result.getOutputResult().getNumFiles());
            assertEquals(10, result.getCounters().getCounter(CounterNames.MAPPER_CALLS).getValue());
            ArrayList<Long> results = new ArrayList<>();
            GcsService gcsService = GcsServiceFactory.createGcsService();
            ByteBuffer holder = ByteBuffer.allocate(8);
            for (GcsFilename file : result.getOutputResult().getAllFiles()) {
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

  @SuppressWarnings({"unchecked"})
  public void testLifeCycleMethodsCalled() throws Exception {
    Input<Long> mockInput = createStrictMock(Input.class);
    InputReader<Long> inputReader = createStrictMock(InputReader.class);
    Output<ByteBuffer, Void> mockOutput = createStrictMock(Output.class);
    OutputWriter<ByteBuffer> outputWriter = createStrictMock(OutputWriter.class);

    @SuppressWarnings("rawtypes")
    List readers = ImmutableList.of(inputReader);
    expect(mockInput.createReaders()).andReturn(readers);
    expect(inputReader.estimateMemoryRequirement()).andReturn(0L).anyTimes();
    inputReader.beginShard();
    inputReader.beginSlice();
    expect(inputReader.next()).andThrow(new NoSuchElementException());
    inputReader.endSlice();
    inputReader.endShard();

    expect(mockOutput.getNumShards()).andReturn(1).times(0, Integer.MAX_VALUE);
    @SuppressWarnings("rawtypes")
    List writers = ImmutableList.of(outputWriter);
    expect(mockOutput.createWriters()).andReturn(writers);
    expect(outputWriter.estimateMemoryRequirement()).andReturn(0L).anyTimes();
    outputWriter.beginShard();
    outputWriter.beginSlice();
    outputWriter.endSlice();
    outputWriter.endShard();
    expect(mockOutput.finish(isA(Collection.class))).andReturn(null);
    replay(mockInput, inputReader, mockOutput, outputWriter);
    runWithPipeline(new Preparer(),
        MapReduceSpecification.of("testLifeCycleMethodsCalled",
            mockInput,
            new LongToBytesMapper(),
            Marshallers.getByteBufferMarshaller(),
            Marshallers.getByteBufferMarshaller(),
            ValueProjectionReducer.<ByteBuffer, ByteBuffer>create(),
            mockOutput),
        new NopVerifier<Void>());
  }

  public void testDatastoreData() throws Exception {
    final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    runTest(new Preparer() {
          @Override
          public MapReduceSettings prepare() {
            // Datastore restriction: id cannot be zero.
            for (long i = 1; i <= 100; ++i) {
              datastoreService.put(new Entity(KeyFactory.createKey("Test", i)));
            }
            return super.prepare();
          }
        },
        MapReduceSpecification.of("Test MR",
            new DatastoreInput("Test", 5),
            new TestMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new TestReducer(),
            new InMemoryOutput<KeyValue<String, List<Long>>>(1)),
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
    runTest(new Preparer(),
        MapReduceSpecification.of("Test MR",
            new DatastoreInput("Test", 2),
            new TestMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, Void>create(),
            NoOutput.<Void, Void>create(1)),
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
    runTest(new Preparer(),
        MapReduceSpecification.of("Test MR",
            new ConsecutiveLongInput(-10000, 10000, 10),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new TestReducer(),
            new InMemoryOutput<KeyValue<String, List<Long>>>(5)),
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
    runTest(new Preparer(),
        MapReduceSpecification.of("Test MR",
            new ConsecutiveLongInput(-10000, 10000, 10),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            KeyProjectionReducer.<String, Long>create(),
            new InMemoryOutput<String>(5)),
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

  @SuppressWarnings("serial")
  static class SideOutputMapper extends Mapper<Long, String, Void> {
    transient BlobFileOutputWriter sideOutput;

    @Override
    public void beginSlice() {
      sideOutput = BlobFileOutputWriter.forWorker(this, "test file", "application/octet-stream");
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

    @SuppressWarnings("deprecation")
    @Override
    public void endSlice() {
      log.info("endShard() in shard " + getContext().getShardNumber());
      try {
        sideOutput.endShard();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      emit(sideOutput.getFile().getFullPath(), null);
    }
  }

  public void testSideOutput() throws Exception {
    runTest(new Preparer(),
        MapReduceSpecification.of("Test MR",
            new ConsecutiveLongInput(0, 6, 6),
            new SideOutputMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getVoidMarshaller(),
            KeyProjectionReducer.<String, Void>create(),
            new InMemoryOutput<String>(1)),
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
                try (@SuppressWarnings("deprecation") FileReadChannel ch =
                    FileServiceFactory.getFileService().openReadChannel(
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
