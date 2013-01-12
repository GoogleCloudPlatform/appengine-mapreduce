// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileReadChannel;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileStat;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.GSFileOptions;
import com.google.appengine.api.files.LockException;
import com.google.appengine.api.files.RecordReadChannel;
import com.google.appengine.api.files.RecordWriteChannel;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.mapreduce.ReducerInput;
import com.google.appengine.tools.mapreduce.inputs.ConsecutiveLongInput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.KeyProjectionReducer;

import junit.framework.TestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 *
 */
public class InMemoryShuffleJobTest extends TestCase {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(InMemoryShuffleJobTest.class.getName());

  private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(
          new LocalFileServiceTestConfig(),
          new LocalBlobstoreServiceTestConfig());

  private final Marshaller<Integer> intMarshaller = Marshallers.getIntegerMarshaller();

  private final String mrJobId = "JobId";
  private final int inputSize = 1000;
  private final int valueRange = 100;
  private final int numShards = 10;

  private FileService fileService;

  private final Mapper<Long, Integer, Integer> mapper = new Mapper<Long, Integer, Integer>() {
    private static final long serialVersionUID = -7327871496359562255L;
    @Override
    public void map(Long value) {
      getContext().emit(mapToRand(value), value.intValue());
    }
  };

  private ConsecutiveLongInput input;
  private MapReduceSpecification<Long, Integer, Integer, Integer, Void> specification;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    input = new ConsecutiveLongInput(-inputSize, inputSize, numShards);
    specification = MapReduceSpecification.<Long, Integer, Integer, Integer, Void>of("JobName",
        input,
        mapper,
        intMarshaller,
        intMarshaller,
        KeyProjectionReducer.<Integer, Integer>create(),
        NoOutput.<Integer, Void>create(numShards));
    helper.setUp();
    fileService = FileServiceFactory.getFileService();
  }

  @Override
  protected void tearDown() throws Exception {
    helper.tearDown();
    super.tearDown();
  }

  private static class ThrowingFileWriteChannel implements FileWriteChannel {
    private final FileWriteChannel wrapped;
    private final double throwProb;
    private MockFileService mockFileService;

    private ThrowingFileWriteChannel(
        MockFileService mockFileService, FileWriteChannel wrapped, double throwProb) {
      this.mockFileService = mockFileService;
      this.wrapped = wrapped;
      this.throwProb = throwProb;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throwMaybe();
      mockFileService.writeCounter.incrementAndGet();
      return wrapped.write(src);
    }

    private void throwMaybe() throws IOException {
      if (mockFileService.r.nextDouble() < throwProb) {
        mockFileService.throwCounter.incrementAndGet();
        throw new IOException();
      }
    }

    @Override
    public boolean isOpen() {
      return wrapped.isOpen();
    }

    @Override
    public void close() throws IOException {
      throwMaybe();
      wrapped.close();
    }

    @Override
    public void closeFinally() throws IllegalStateException, IOException {
      throwMaybe();
      wrapped.closeFinally();
    }

    @Override
    public int write(ByteBuffer arg0, String arg1) throws IOException {
      throwMaybe();
      return wrapped.write(arg0, arg1);
    }

  }

  private static class ThrowingFileReadChannel implements FileReadChannel {
    private final FileReadChannel wrapped;
    private final double throwProb;
    private MockFileService mockFileService;

    private ThrowingFileReadChannel(
        MockFileService mockFileService, FileReadChannel toWrapp, double throwProb) {
      this.mockFileService = mockFileService;
      wrapped = toWrapp;
      this.throwProb = throwProb;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      throwMaybe();
      mockFileService.readCounter.incrementAndGet();
      return wrapped.read(dst);
    }

    @Override
    public boolean isOpen() {
      return wrapped.isOpen();
    }

    @Override
    public void close() throws IOException {
      throwMaybe();
      wrapped.close();
    }

    private void throwMaybe() throws IOException {
      if (mockFileService.r.nextDouble() < throwProb) {
        mockFileService.throwCounter.incrementAndGet();
        throw new IOException();
      }
    }

    @Override
    public long position() throws IOException {
      return wrapped.position();
    }

    @Override
    public FileReadChannel position(long arg0) throws IOException {
      throwMaybe();
      return wrapped.position(arg0);
    }

  }

  private static class ThrowingRecordWriteChannel implements RecordWriteChannel {
    private final RecordWriteChannel wrapped;
    private final double throwProb;
    private MockFileService mockFileService;

    private ThrowingRecordWriteChannel(
        MockFileService mockFileService, RecordWriteChannel toWrapp, double throwProb) {
      this.mockFileService = mockFileService;
      wrapped = toWrapp;
      this.throwProb = throwProb;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      throwMaybe();
      mockFileService.writeCounter.incrementAndGet();
      return wrapped.write(src);
    }

    private void throwMaybe() throws IOException {
      if (mockFileService.r.nextDouble() < throwProb) {
        mockFileService.throwCounter.incrementAndGet();
        throw new IOException();
      }
    }

    @Override
    public boolean isOpen() {
      return wrapped.isOpen();
    }

    @Override
    public void close() throws IOException {
      throwMaybe();
      wrapped.close();
    }

    @Override
    public void closeFinally() throws IllegalStateException, IOException {
      throwMaybe();
      wrapped.closeFinally();
    }

    @Override
    public int write(ByteBuffer arg0, String arg1) throws IOException {
      throwMaybe();
      mockFileService.writeCounter.incrementAndGet();
      return wrapped.write(arg0, arg1);
    }
  }



  private static class ThrowingRecordReadChannel implements RecordReadChannel {
    private final RecordReadChannel wrapped;
    private final double throwProb;
    private MockFileService mockFileService;

    private ThrowingRecordReadChannel(
        MockFileService mockFileService, RecordReadChannel toWrapp, double throwProb) {
      this.mockFileService = mockFileService;
      wrapped = toWrapp;
      this.throwProb = throwProb;
    }

    @Override
    public long position() throws IOException {
      return wrapped.position();
    }

    @Override
    public void position(long arg0) throws IOException {
      throwMaybe();
      wrapped.position(arg0);
    }

    @Override
    public ByteBuffer readRecord() throws IOException {
      throwMaybe();
      mockFileService.readCounter.incrementAndGet();
      return wrapped.readRecord();
    }

    private void throwMaybe() throws IOException {
      if (mockFileService.r.nextDouble() < throwProb) {
        mockFileService.throwCounter.incrementAndGet();
        throw new IOException();
      }
    }
  }

  private class MockFileService implements FileService {
    private final AtomicInteger readCounter = new AtomicInteger(0);
    private final AtomicInteger writeCounter = new AtomicInteger(0);
    private final AtomicInteger throwCounter = new AtomicInteger(0);
    private final Random r = new Random(17);
    private final double throwOnRead;
    private final double throwOnWrite;

    public MockFileService(double throwOnRead, double throwOnWrite) {
      this.throwOnRead = throwOnRead;
      this.throwOnWrite = throwOnWrite;
    }

    @Override
    public AppEngineFile createNewBlobFile(String arg0) throws IOException {
      return fileService.createNewBlobFile(arg0);
    }

    @Override
    public AppEngineFile createNewBlobFile(String arg0, String arg1) throws IOException {
      return fileService.createNewBlobFile(arg0, arg1);
    }

    @Override
    public AppEngineFile createNewGSFile(GSFileOptions arg0) throws IOException {
      return fileService.createNewGSFile(arg0);
    }

    @Override
    public AppEngineFile getBlobFile(BlobKey arg0) {
      return fileService.getBlobFile(arg0);
    }

    @Override
    public BlobKey getBlobKey(AppEngineFile arg0) {
      return fileService.getBlobKey(arg0);
    }

    @Override
    public String getDefaultGsBucketName() throws IOException {
      return fileService.getDefaultGsBucketName();
    }

    @Override
    public FileReadChannel openReadChannel(AppEngineFile arg0, boolean arg1)
        throws FileNotFoundException, LockException, IOException {
      return new ThrowingFileReadChannel(
          this, fileService.openReadChannel(arg0, arg1), throwOnRead);
    }

    @Override
    public RecordReadChannel openRecordReadChannel(AppEngineFile arg0, boolean arg1)
        throws FileNotFoundException, LockException, IOException {
      return new ThrowingRecordReadChannel(
          this, fileService.openRecordReadChannel(arg0, arg1), throwOnRead);
    }

    @Override
    public RecordWriteChannel openRecordWriteChannel(AppEngineFile arg0, boolean arg1)
        throws FileNotFoundException, FinalizationException, LockException, IOException {
      return new ThrowingRecordWriteChannel(
          this, fileService.openRecordWriteChannel(arg0, arg1), throwOnWrite);
    }

    @Override
    public FileWriteChannel openWriteChannel(AppEngineFile arg0, boolean arg1)
        throws FileNotFoundException, FinalizationException, LockException, IOException {
      return new ThrowingFileWriteChannel(
          this, fileService.openWriteChannel(arg0, arg1), throwOnWrite);
    }

    @Override
    public FileStat stat(AppEngineFile arg0) throws IOException {
      return fileService.stat(arg0);
    }

    @Override
    public void delete(AppEngineFile... arg0) throws IOException {
      fileService.delete(arg0);
    }
  }


  public int mapToRand(long value) {
    Random rand = new Random(value);
    return rand.nextInt(valueRange);
  }

  private void testShuffler(double throwOnRead, double throwOnWrite) throws IOException {
    Output<KeyValue<Integer, Integer>, List<AppEngineFile>> output = new IntermediateOutput<
        Integer, Integer>(mrJobId, numShards, specification.getIntermediateKeyMarshaller(),
        specification.getIntermediateValueMarshaller());

    List<? extends InputReader<Long>> readers = input.createReaders();
    List<? extends OutputWriter<KeyValue<Integer, Integer>>> writers = output.createWriters();
    assertEquals(numShards, readers.size());
    assertEquals(numShards, writers.size());

    map(mrJobId, numShards, readers, writers);

    List<AppEngineFile> mapOutputs = output.finish(writers);

    ArrayList<AppEngineFile> reduceInputFiles = createInputFiles();

    MockFileService fileService = new MockFileService(throwOnRead, throwOnWrite);
    InMemoryShuffleJob<Integer, Integer, Integer> shuffleJob =
        new InMemoryShuffleJob<Integer, Integer, Integer>(
            specification, fileService);

    shuffleJob.run(mapOutputs, reduceInputFiles, new ShuffleResult<Integer, Integer, Integer>(
        reduceInputFiles, Collections.<OutputWriter<Integer>>emptyList(),
        Collections.<InputReader<KeyValue<Integer, ReducerInput<Integer>>>>emptyList()));

    IntermediateInput<Integer, Integer> reduceInput =
        new IntermediateInput<Integer, Integer>(reduceInputFiles, intMarshaller, intMarshaller);

    int totalCount = assertShuffledCorrectly(reduceInput, mapper);
    assertEquals(2 * inputSize, totalCount);
    assertEquals(2 * inputSize + numShards, fileService.readCounter.get());
    assertEquals(valueRange, fileService.writeCounter.get());
    assertTrue(throwOnRead <= 0 || fileService.throwCounter.get() > 0);
    assertTrue(throwOnWrite <= 0 || fileService.throwCounter.get() > 0);
  }

  private ArrayList<AppEngineFile> createInputFiles() throws IOException {
    ArrayList<AppEngineFile> reduceInputFiles = new ArrayList<AppEngineFile>();
    for (int i = 0; i < numShards; i++) {
      reduceInputFiles.add(fileService.createNewBlobFile(
          MapReduceConstants.REDUCE_INPUT_MIME_TYPE, mrJobId + ": reduce input, shard " + i));
    }
    return reduceInputFiles;
  }

  public void testShuffler() throws IOException {
    testShuffler(0, 0);
  }

  public void testAllReadsFail() throws IOException {
    try {
      testShuffler(1, 0);
      fail();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().startsWith("RetryHelper"));
      assertEquals(IOException.class, e.getCause().getClass());
    }
  }

  public void testReadFailOften() throws IOException {
    testShuffler(.1, 0);
  }

  public void testReadFailSometimes() throws IOException {
    testShuffler(.05, 0);
  }

  public void testReadFailRarely() throws IOException {
    testShuffler(.025, 0);
  }

  public void testAllWritesFail() throws IOException {
    try {
      testShuffler(0, 1);
      fail();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().startsWith("RetryHelper"));
      assertEquals(IOException.class, e.getCause().getClass());
    }
  }

  public void testWritesFailOften() throws IOException {
    testShuffler(0, 0.5);
  }

  public void testWritesFailSometimes() throws IOException {
    testShuffler(0, .2);
  }

  public void testWritesFailRarely() throws IOException {
    testShuffler(0, .05);
  }

  private void map(String mrJobId, int numShards, List<? extends InputReader<Long>> readers,
      List<? extends OutputWriter<KeyValue<Integer, Integer>>> writers) {
    InProcessMapReduce<Long, Integer, Integer, Integer, Void> impl =
        new InProcessMapReduce<Long, Integer, Integer, Integer, Void>(mrJobId, specification);
    impl.map(readers, writers);
  }

  private int assertShuffledCorrectly(
      IntermediateInput<Integer, Integer> reducerInput, Mapper<Long, Integer, Integer> mapper)
      throws IOException {
    int count = 0;
    for (InputReader<KeyValue<Integer, ReducerInput<Integer>>> reader :
        reducerInput.createReaders()) {
      while (true) {
        KeyValue<Integer, ReducerInput<Integer>> kv;
        try {
          kv = reader.next();
        } catch (NoSuchElementException e) {
          break;
        }
        ReducerInput<Integer> iter = kv.getValue();
        assertTrue(iter.hasNext());
        while (iter.hasNext()) {
          Integer value = iter.next();
          Integer key = mapToRand(value.longValue());
          assertEquals(kv.getKey(), key);
          count++;
        }
      }
    }
    return count;
  }

}
