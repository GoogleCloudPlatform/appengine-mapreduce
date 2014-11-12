// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.appengine.tools.mapreduce.servlets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.google.appengine.api.blobstore.dev.LocalBlobstoreService;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.development.ApiProxyLocal;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalFileServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalModulesServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig.ServletInvokingTaskCallback;
import com.google.appengine.tools.mapreduce.KeyValue;
import com.google.appengine.tools.mapreduce.MapReduceServlet;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Marshallers;
import com.google.appengine.tools.mapreduce.impl.sort.LexicographicalComparator;
import com.google.appengine.tools.mapreduce.inputs.GoogleCloudStorageLevelDbInputReader;
import com.google.appengine.tools.mapreduce.outputs.GoogleCloudStorageFileOutputWriter;
import com.google.appengine.tools.mapreduce.outputs.LevelDbOutputWriter;
import com.google.appengine.tools.mapreduce.servlets.ShufflerServlet.ShuffleMapReduce;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;
import com.google.appengine.tools.pipeline.impl.servlets.PipelineServlet;
import com.google.apphosting.api.ApiProxy;
import com.google.common.collect.TreeMultimap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Tests for {@link ShufflerServlet}
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class ShufflerServletTest {

  private static final Logger log = Logger.getLogger(ShufflerServletTest.class.getName());

  private static final String CALLBACK_PATH = "/callback";

  private static final int maxRecordSize = 500;

  private static Marshaller<KeyValue<ByteBuffer, ByteBuffer>> KEY_VALUE_MARSHALLER =
      Marshallers.getKeyValueMarshaller(Marshallers.getByteBufferMarshaller(),
          Marshallers.getByteBufferMarshaller());

  private static
      Marshaller<KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>>> KEY_VALUES_MARSHALLER =
          Marshallers.getKeyValuesMarshaller(Marshallers.getByteBufferMarshaller(),
              Marshallers.getByteBufferMarshaller());
  private final int recordsPerFile = 10;

  private final static Semaphore WAIT_ON = new Semaphore(0);

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig(), new LocalTaskQueueTestConfig()
          .setDisableAutoTaskExecution(false).setCallbackClass(TaskRunner.class),
      new LocalMemcacheServiceTestConfig(), new LocalFileServiceTestConfig(),
      new LocalModulesServiceTestConfig());



  public static class TaskRunner extends ServletInvokingTaskCallback {
    private final static Map<String, HttpServlet> servletMap = new HashMap<>();
    static {
      servletMap.put("/mapreduce", new MapReduceServlet());
      servletMap.put("/_ah/pipeline", new PipelineServlet());
      servletMap.put(CALLBACK_PATH, new CallbackServlet());
    }

    @Override
    protected Map<String, HttpServlet> getServletMap() {
      return servletMap;
    }

    @Override
    protected HttpServlet getDefaultServlet() {
      return new HttpServlet() {};
    }
  }

  private static class CallbackServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
        IOException {
      WAIT_ON.release();
    }
  }

  @Before
  public void setUp() throws Exception {
    helper.setUp();
    ApiProxyLocal proxy = (ApiProxyLocal) ApiProxy.getDelegate();
    // Creating files is not allowed in some test execution environments, so don't.
    proxy.setProperty(LocalBlobstoreService.NO_STORAGE_PROPERTY, "true");
    WAIT_ON.drainPermits();
  }

  @After
  public void tearDown() throws Exception {
    for (int count = 0; getQueueDepth() > 0; count++) {
      if (count > 10) {
        log.severe("Messages did not drain from queue.");
        break;
      }
      Thread.sleep(1000);
    }
    helper.tearDown();
  }

  private int getQueueDepth() {
    return LocalTaskQueueTestConfig
        .getLocalTaskQueue()
        .getQueueStateInfo()
        .get(QueueFactory.getDefaultQueue().getQueueName())
        .getTaskInfo()
        .size();
  }

  @Test
  public void testDataIsOrdered() throws InterruptedException, IOException {
    ShufflerParams shufflerParams = createParams(3, 10);
    TreeMultimap<ByteBuffer, ByteBuffer> input = writeInputFiles(shufflerParams, new Random(0));
    PipelineService service = PipelineServiceFactory.newPipelineService();
    ShuffleMapReduce mr = new ShuffleMapReduce(shufflerParams);
    String pipelineId = service.startNewPipeline(mr);
    assertTrue(WAIT_ON.tryAcquire(100, TimeUnit.SECONDS));
    List<KeyValue<ByteBuffer, List<ByteBuffer>>> output =
        validateOrdered(shufflerParams, mr, pipelineId);
    assertExpectedOutput(input, output);
  }

  @Test
  public void testJson() throws IOException {
    ShufflerParams shufflerParams = createParams(3, 10);
    Marshaller<ShufflerParams> marshaller =
        Marshallers.getGenericJsonMarshaller(ShufflerParams.class);
    ByteBuffer bytes = marshaller.toBytes(shufflerParams);
    ByteArrayInputStream bin = new ByteArrayInputStream(bytes.array());
    ShufflerParams readShufflerParams = ShufflerServlet.readShufflerParams(bin);
    assertEquals(shufflerParams.getShufflerQueue(), readShufflerParams.getShufflerQueue());
    assertEquals(shufflerParams.getGcsBucket(), readShufflerParams.getGcsBucket());
    assertArrayEquals(shufflerParams.getInputFileNames(), readShufflerParams.getInputFileNames());
    assertEquals(shufflerParams.getOutputDir(), readShufflerParams.getOutputDir());
    assertEquals(shufflerParams.getOutputShards(), readShufflerParams.getOutputShards());
    assertEquals(shufflerParams.getCallbackQueue(), readShufflerParams.getCallbackQueue());
    assertEquals(shufflerParams.getCallbackModule(), readShufflerParams.getCallbackModule());
    assertEquals(shufflerParams.getCallbackVersion(), readShufflerParams.getCallbackVersion());
    assertEquals(shufflerParams.getCallbackPath(), readShufflerParams.getCallbackPath());
  }

  private void assertExpectedOutput(TreeMultimap<ByteBuffer, ByteBuffer> expected,
      List<KeyValue<ByteBuffer, List<ByteBuffer>>> actual) {
    for (KeyValue<ByteBuffer, List<ByteBuffer>> kv : actual) {
      SortedSet<ByteBuffer> expectedValues = expected.removeAll(kv.getKey());
      assertTrue(expectedValues.containsAll(kv.getValue()));
      assertTrue(kv.getValue().containsAll(expectedValues));
    }
    assertTrue(expected.isEmpty());
  }

  static List<KeyValue<ByteBuffer, List<ByteBuffer>>> validateOrdered(ShufflerParams shufflerParams,
      ShuffleMapReduce mr, String pipelineId) throws IOException {
    List<KeyValue<ByteBuffer, List<ByteBuffer>>> result = new ArrayList<>();
    String outputNamePattern = mr.getOutputNamePattern(pipelineId);
    for (int shard = 0; shard < shufflerParams.getOutputShards(); shard++) {
      String fileName = String.format(outputNamePattern, shard);
      GoogleCloudStorageLevelDbInputReader reader = new GoogleCloudStorageLevelDbInputReader(
          new GcsFilename(shufflerParams.getGcsBucket(), fileName), 1024 * 1024);
      reader.beginShard();
      reader.beginSlice();
      try {
        ByteBuffer previous = null;
        while (true) {
          KeyValue<ByteBuffer, ? extends Iterable<ByteBuffer>> keyValue =
              KEY_VALUES_MARSHALLER.fromBytes(reader.next());
          assertTrue(previous == null
              || LexicographicalComparator.compareBuffers(previous, keyValue.getKey()) < 0);
          ArrayList<ByteBuffer> list = new ArrayList<>();
          for (ByteBuffer item : keyValue.getValue()) {
            list.add(item);
          }
          result.add(new KeyValue<ByteBuffer, List<ByteBuffer>>(keyValue.getKey(), list));
        }
      } catch (NoSuchElementException e) {
      }
      reader.endSlice();
      reader.endShard();
    }
    return result;
  }

  private TreeMultimap<ByteBuffer, ByteBuffer> writeInputFiles(ShufflerParams shufflerParams,
      Random rand) throws IOException {
    LexicographicalComparator comparator = new LexicographicalComparator();
    TreeMultimap<ByteBuffer, ByteBuffer> result = TreeMultimap.create(comparator, comparator);
    for (String fileName : shufflerParams.getInputFileNames()) {
      LevelDbOutputWriter writer = new LevelDbOutputWriter(new GoogleCloudStorageFileOutputWriter(
          new GcsFilename(shufflerParams.getGcsBucket(), fileName), "testData"));
      writer.beginShard();
      writer.beginSlice();
      for (int i = 0; i < recordsPerFile; i++) {
        KeyValue<ByteBuffer, ByteBuffer> kv = writeRandomKVByteBuffer(rand, writer);
        result.put(kv.getKey(), kv.getValue());
      }
      writer.endSlice();
      writer.endShard();
    }
    return result;
  }

  private KeyValue<ByteBuffer, ByteBuffer> writeRandomKVByteBuffer(Random rand,
      LevelDbOutputWriter writer) throws IOException {
    byte[] key = new byte[rand.nextInt(5) + 1];
    byte[] value = new byte[rand.nextInt(maxRecordSize)];
    rand.nextBytes(key);
    rand.nextBytes(value);
    KeyValue<ByteBuffer, ByteBuffer> keyValue =
        new KeyValue<>(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
    writer.write(KEY_VALUE_MARSHALLER.toBytes(keyValue));
    return keyValue;
  }

  static ShufflerParams createParams(int inputFiles, int outputShards) {
    ShufflerParams shufflerParams = new ShufflerParams();
    shufflerParams.setCallbackModule("default");
    shufflerParams.setCallbackVersion("callbackVersion");
    shufflerParams.setCallbackPath(CALLBACK_PATH);
    shufflerParams.setCallbackQueue("default");
    ArrayList<String> list = new ArrayList<>();
    for (int i = 0; i < inputFiles; i++) {
      list.add("input" + i);
    }
    shufflerParams.setInputFileNames(list.toArray(new String[inputFiles]));
    shufflerParams.setOutputShards(outputShards);
    shufflerParams.setShufflerQueue("default");
    shufflerParams.setGcsBucket("storageBucket");
    shufflerParams.setOutputDir("storageDir");
    return shufflerParams;
  }

}
