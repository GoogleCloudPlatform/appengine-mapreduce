package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.EndToEndTest.TestMapper;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.reducers.ValueProjectionReducer;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Tests that custom output classes work.
 */
public class CustomOutputTest extends EndToEndTestCase {

  private PipelineService pipelineService;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    pipelineService = PipelineServiceFactory.newPipelineService();
  }

  @SuppressWarnings("serial")
  static class CustomWriter extends OutputWriter<Long> {
    final int id;

    CustomWriter(int id) {
      this.id = id;
    }

    @Override
    public void write(Long value) throws IOException {
      //Do nothing
    }

    @Override
    public void close() throws IOException {
      //Do nothing
    }
  }

  @SuppressWarnings("serial")
  static class CustomOutput extends Output<Long, Boolean> {

    @Override
    public List<? extends OutputWriter<Long>> createWriters() {
      List<CustomWriter> result = new ArrayList<>(getNumShards());
      for (int i=0;i<getNumShards();i++) {
        result.add(new CustomWriter(i));
      }
      return result;
    }

    @Override
    public int getNumShards() {
      return 17;
    }

    @Override
    public Boolean finish(Collection<? extends OutputWriter<Long>> writers) throws IOException {
      Iterator<? extends OutputWriter<Long>> iter = writers.iterator();
      for (int i=0;i<getNumShards();i++) {
        CustomWriter writer = (CustomWriter) iter.next();
        if (writer.id != i) {
          return false;
        }
      }
      if (iter.hasNext()) {
        return false;
      }
      return true;
    }
  }

  public void testOutputInOrder() throws Exception {
    MapReduceSpecification<Entity, String, Long, Long, Boolean> mrSpec =
        MapReduceSpecification.of("Test MR", new DatastoreInput("Test", 2), new TestMapper(),
        Marshallers.getStringMarshaller(), Marshallers.getLongMarshaller(),
        ValueProjectionReducer.<String, Long>create(), new CustomOutput());
    MapReduceSettings mrSettings = new MapReduceSettings();
    String jobId = pipelineService.startNewPipeline(
        new MapReduceJob<Entity, String, Long, Long, Boolean>(), mrSpec, mrSettings);
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    MapReduceResult<Boolean> result = (MapReduceResult<Boolean>) info.getOutput();
    assertNotNull(result);
    assertTrue(result.getOutputResult());
  }
}
