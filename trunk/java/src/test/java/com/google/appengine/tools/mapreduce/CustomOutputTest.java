package com.google.appengine.tools.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.mapreduce.EndToEndTest.TestMapper;
import com.google.appengine.tools.mapreduce.inputs.DatastoreInput;
import com.google.appengine.tools.mapreduce.reducers.ValueProjectionReducer;
import com.google.appengine.tools.pipeline.JobInfo;
import com.google.appengine.tools.pipeline.JobInfo.State;
import com.google.appengine.tools.pipeline.PipelineService;
import com.google.appengine.tools.pipeline.PipelineServiceFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Tests that custom output classes work.
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class CustomOutputTest extends EndToEndTestCase {

  @SuppressWarnings("serial")
  static class CustomWriter extends OutputWriter<Long> {
    final int id;

    CustomWriter(int id) {
      this.id = id;
    }

    @Override
    public void write(Long value) {
      //Do nothing
    }

    @Override
    public void endShard() {
      //Do nothing
    }
  }

  @SuppressWarnings("serial")
  static class CustomOutput extends Output<Long, Boolean> {

    private static int numShards;

    @Override
    public List<? extends OutputWriter<Long>> createWriters(int numShards) {
      CustomOutput.numShards = numShards;
      List<CustomWriter> result = new ArrayList<>(numShards);
      for (int i = 0; i < numShards; i++) {
        result.add(new CustomWriter(i));
      }
      return result;
    }

    @Override
    public Boolean finish(Collection<? extends OutputWriter<Long>> writers) {
      Iterator<? extends OutputWriter<Long>> iter = writers.iterator();
      for (int i = 0; i < CustomOutput.numShards; i++) {
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

  @Test
  public void testOutputInOrder() throws Exception {
    MapReduceSpecification.Builder<Entity, String, Long, Long, Boolean> mrSpecBuilder =
        new MapReduceSpecification.Builder<>();
    mrSpecBuilder.setJobName("Test MR").setInput(new DatastoreInput("Test", 2))
        .setMapper(new TestMapper()).setKeyMarshaller(Marshallers.getStringMarshaller())
        .setValueMarshaller(Marshallers.getLongMarshaller())
        .setReducer(ValueProjectionReducer.<String, Long>create())
        .setOutput(new CustomOutput())
        .setNumReducers(17);
    PipelineService pipelineService = PipelineServiceFactory.newPipelineService();
    MapReduceSettings mrSettings = new MapReduceSettings.Builder().build();
    String jobId = pipelineService.startNewPipeline(
        new MapReduceJob<>(mrSpecBuilder.build(), mrSettings));
    assertFalse(jobId.isEmpty());
    executeTasksUntilEmpty("default");
    JobInfo info = pipelineService.getJobInfo(jobId);
    assertEquals(State.COMPLETED_SUCCESSFULLY, info.getJobState());
    @SuppressWarnings("unchecked")
    MapReduceResult<Boolean> result = (MapReduceResult<Boolean>) info.getOutput();
    assertNotNull(result);
    assertTrue(result.getOutputResult());
  }
}
