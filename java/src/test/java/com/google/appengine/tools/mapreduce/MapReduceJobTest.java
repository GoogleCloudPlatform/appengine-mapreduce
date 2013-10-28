// Copyright 2013 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.taskqueue.IQueueFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.spi.FactoryProvider;
import com.google.appengine.spi.ServiceFactoryFactory;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.mapreduce.inputs.NoInput;
import com.google.appengine.tools.mapreduce.outputs.NoOutput;
import com.google.appengine.tools.mapreduce.reducers.NoReducer;

import junit.framework.TestCase;

import org.easymock.EasyMock;

/**
 */
public class MapReduceJobTest extends TestCase {

    private final LocalServiceTestHelper helper =
      new LocalServiceTestHelper(new LocalDatastoreServiceTestConfig());

    private static class DummyMapper extends Mapper<Long, String, Long> {
      @Override public void map(Long value) {}
    }

    @Override
    public void setUp() {
      helper.setUp();
    }

    public void testBadQueueSettings() throws Exception {
      ServiceFactoryFactory.register(new FactoryProvider<IQueueFactory>(IQueueFactory.class) {
        @Override
        protected IQueueFactory getFactoryInstance() {
          return new IQueueFactory() {
            @Override
            public Queue getQueue(String queueName) {
              Queue queue = EasyMock.createMock(Queue.class);
              if (queueName.startsWith("bad-")) {
                EasyMock.expect(queue.fetchStatistics()).andThrow(new IllegalStateException());
              } else {
                EasyMock.expect(queue.fetchStatistics()).andReturn(null);
              }
              EasyMock.replay(queue);
              return queue;
            }
          };
        }
      });

    MapReduceSettings settings = new MapReduceSettings();
    settings.setControllerQueueName("bad-queue");
    MapReduceSpecification<Long, String, Long, String, String> specification =
        MapReduceSpecification.of("Empty test MR", new NoInput<Long>(1), new DummyMapper(),
            Marshallers.getStringMarshaller(), Marshallers.getLongMarshaller(),
            NoReducer.<String, Long, String>create(), new NoOutput<String, String>(1));
    try {
      MapReduceJob.start(specification, settings);
      fail("was expecting failure due to bad queue");
    } catch (RuntimeException ex) {
      assertEquals("Queue 'bad-queue' does not exists", ex.getMessage());
    }
  }
}