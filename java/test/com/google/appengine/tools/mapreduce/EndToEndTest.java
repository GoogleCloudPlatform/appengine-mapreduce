// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.tools.mapreduce.impl.InProcessMapReduce;
import com.google.appengine.tools.mapreduce.impl.Shuffling;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.outputs.BlobFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.InMemoryOutput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.KeyProjectionReducer;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 */
public class EndToEndTest extends EndToEndTestCase {

  private static final Logger log = Logger.getLogger(EndToEndTest.class.getName());

  private PipelineService pipelineService;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    pipelineService = PipelineServiceFactory.newPipelineService();
  }

  private interface Preparer {
    void prepare() throws Exception;
  }

  private interface Verifier<R> {
    void verify(MapReduceResult<R> result) throws Exception;
  }

  // (runWithPipeline is also in-process in our test setup, so this is a misnomer.)
  private <I, K, V, O, R> void runInProcess(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    preparer.prepare();
    verifier.verify(InProcessMapReduce.runMapReduce(mrSpec));
  }

  private <I, K, V, O, R> void runWithPipeline(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    preparer.prepare();
    String jobId = pipelineService.startNewPipeline(new MapReduceJob<I, K, V, O, R>(),
        mrSpec, new MapReduceSettings());
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    @SuppressWarnings("unchecked")
    MapReduceResult<R> result = (MapReduceResult) info.getOutput();
    assertNotNull(result);
    verifier.verify(result);
  }

  private <I, K, V, O, R> void runTest(Preparer preparer,
      MapReduceSpecification<I, K, V, O, R> mrSpec, Verifier<R> verifier) throws Exception {
    runInProcess(preparer, mrSpec, verifier);
    runWithPipeline(preparer, mrSpec, verifier);
  }

  public void testEmpty() throws Exception {
    runTest(
        new Preparer() {
          @Override public void prepare() throws Exception {
          }
        },
        MapReduceSpecification.of("Empty test MR",
            new ConsecutiveLongInput(0, 0, 1),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new TestReducer(),
            new InMemoryOutput<KeyValue<String, List<Long>>>(1)),
        new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
          @Override public void verify(
              MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
              throws Exception {
            List<KeyValue<String, List<Long>>> output = Iterables.getOnlyElement(
                result.getOutputResult());
            assertEquals(ImmutableList.of(), output);
          }
        });
  }

  public void testDatastoreData() throws Exception {
    final DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
    runTest(
        new Preparer() {
          @Override public void prepare() throws Exception {
            // Datastore restriction: id cannot be zero.
            for (long i = 1; i <= 100; ++i) {
              datastoreService.put(new Entity(KeyFactory.createKey("Test", i)));
            }
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
          @Override public void verify(
              MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
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

            List<KeyValue<String, List<Long>>> output = Iterables.getOnlyElement(
                result.getOutputResult());
            List<KeyValue<String, ImmutableList<Long>>> expectedOutput = ImmutableList.of(
                KeyValue.of("even",
                    ImmutableList.of(
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
                        100L)),
                KeyValue.of("multiple-of-ten",
                    ImmutableList.of(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L)));
            assertEquals(expectedOutput, output);
          }
        });
  }

  public void testNoData() throws Exception {
    runTest(
        new Preparer() {
          @Override public void prepare() throws Exception {
          }
        },
        MapReduceSpecification.of("Test MR",
            new DatastoreInput("Test", 2),
            new TestMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, Void>create(),
            NoOutput.<Void, Void>create(1)),
        new Verifier<Void>() {
          @Override public void verify(MapReduceResult<Void> result) throws Exception {
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

  private static class Mod37Mapper extends Mapper<Long, String, Long> {
    @Override public void map(Long input) {
      String mod37 = "" + (Math.abs(input) % 37);
      getContext().emit(mod37, input);
    }
  }

  public void testSomeNumbers() throws Exception {
    runTest(
        new Preparer() {
          @Override public void prepare() throws Exception {
          }
        },
        MapReduceSpecification.of("Test MR",
            new ConsecutiveLongInput(-1000, 1000, 10),
            new Mod37Mapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getLongMarshaller(),
            new TestReducer(),
            new InMemoryOutput<KeyValue<String, List<Long>>>(5)),
        new Verifier<List<List<KeyValue<String, List<Long>>>>>() {
          @Override public void verify(
              MapReduceResult<List<List<KeyValue<String, List<Long>>>>> result)
              throws Exception {
            Counters counters = result.getCounters();
            assertEquals(2000, counters.getCounter(CounterNames.MAPPER_CALLS).getValue());
            assertEquals(37, counters.getCounter(CounterNames.REDUCER_CALLS).getValue());

            List<List<KeyValue<String, List<Long>>>> actualOutput = result.getOutputResult();
            List<ArrayListMultimap<String, Long>> expectedOutput = Lists.newArrayList();
            for (int i = 0; i < 5; i++) {
              expectedOutput.add(ArrayListMultimap.<String, Long>create());
            }
            Marshaller<String> marshaller = Marshallers.getStringMarshaller();
            for (long l = -1000; l < 1000; l++) {
              String mod37 = "" + (Math.abs(l) % 37);
              expectedOutput.get(Shuffling.reduceShardFor(marshaller.toBytes(mod37), 5))
                  .put(mod37, l);
            }
            for (int i = 0; i < 5; i++) {
              assertEquals(expectedOutput.get(i).keySet().size(), actualOutput.get(i).size());
              for (KeyValue<String, List<Long>> actual : actualOutput.get(i)) {
                assertEquals(
                    "shard " + i + ", key " + actual.getKey(),
                    expectedOutput.get(i).get(actual.getKey()), actual.getValue());
              }
            }
          }
        });
  }

  static class SideOutputMapper extends Mapper<Long, String, Void> {
    transient BlobFileOutputWriter sideOutput;

    @Override public void beginShard() {
      sideOutput = BlobFileOutputWriter.forWorker(this, "test file", "application/octet-stream");
    }

    @Override public void map(Long input) {
      log.info("map(" + input + ") in shard " + getContext().getShardNumber());
      try {
        sideOutput.write(Marshallers.getLongMarshaller().toBytes(input));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public void endShard() {
      log.info("endShard() in shard " + getContext().getShardNumber());
      try {
        sideOutput.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      getContext().emit(sideOutput.getFile().getFullPath(), null);
    }
  }

  public void testSideOutput() throws Exception {
    runTest(
        new Preparer() {
          @Override public void prepare() throws Exception {
          }
        },
        MapReduceSpecification.of("Test MR",
            new ConsecutiveLongInput(0, 6, 6),
            new SideOutputMapper(),
            Marshallers.getStringMarshaller(),
            Marshallers.getVoidMarshaller(),
            KeyProjectionReducer.<String, Void>create(),
            new InMemoryOutput<String>(1)),
        new Verifier<List<List<String>>>() {
          @Override public void verify(MapReduceResult<List<List<String>>> result)
              throws Exception {
            List<List<String>> outputResult = result.getOutputResult();
            Set<Long> expected = Sets.newHashSet();
            for (long i = 0; i < 6; i++) {
              expected.add(i);
            }
            assertEquals(1, outputResult.size());
            for (List<String> files : outputResult) {
              assertEquals(6, files.size());
              for (String file : files) {
                ByteBuffer buf = ByteBuffer.allocate(8);
                FileReadChannel ch =
                    FileServiceFactory.getFileService().openReadChannel(
                        new AppEngineFile(file), false);
                assertEquals(8, ch.read(buf));
                assertEquals(-1, ch.read(ByteBuffer.allocate(1)));
                ch.close();
                buf.flip();
                assertTrue(expected.remove(Marshallers.getLongMarshaller().fromBytes(buf)));
              }
            }
            assertTrue(expected.isEmpty());
          }
        });
  }

  static class TestMapper extends Mapper<Entity, String, Long> {
    private transient DatastoreMutationPool pool;

    @Override public void map(Entity entity) {
      getContext().incrementCounter("map");
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      long key = entity.getKey().getId();
      log.info("map(" + key + ")");
      if (key % 2 == 0) {
        getContext().emit("even", key);
      }
      if (key % 10 == 0) {
        getContext().emit("multiple-of-ten", key);
      }

      DatastoreMutationPool pool = DatastoreMutationPool.forWorker(this);
      entity.setProperty("mark", Boolean.TRUE);
      pool.put(entity);
    }

    @Override public void beginShard() {
      getContext().incrementCounter("beginShard");
    }

    @Override public void endShard() {
      getContext().incrementCounter("endShard");
    }

    @Override public void beginSlice() {
      getContext().incrementCounter("beginSlice");
      pool = DatastoreMutationPool.forWorker(this);
    }

    @Override public void endSlice() {
      getContext().incrementCounter("endSlice");
    }
  }

  static class TestReducer extends Reducer<String, Long, KeyValue<String, List<Long>>> {
    @Override public void reduce(String property, ReducerInput<Long> matchingValues) {
      ImmutableList.Builder<Long> out = ImmutableList.builder();
      while (matchingValues.hasNext()) {
        long value = matchingValues.next();
        getContext().incrementCounter(property);
        out.add(value);
      }
      List<Long> values = out.build();
      getContext().emit(KeyValue.of(property, values));
    }
  }

}
