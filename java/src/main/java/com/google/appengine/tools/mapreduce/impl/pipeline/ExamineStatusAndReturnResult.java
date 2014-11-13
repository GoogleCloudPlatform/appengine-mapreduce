package com.google.appengine.tools.mapreduce.impl.pipeline;

import com.google.appengine.tools.mapreduce.MapReduceJobException;
import com.google.appengine.tools.mapreduce.MapReduceResult;
import com.google.appengine.tools.mapreduce.impl.shardedjob.Status;
import com.google.appengine.tools.pipeline.Job1;
import com.google.appengine.tools.pipeline.Value;

/**
 * A pipeline job that examines {@code ResultAndStatus} and returns {@code MapReduceResult}
 * when status is DONE or throw a {@code MapReduceJobException} otherwise.
 *
 * @param <R> the type of MapReduceResult content
 */
 // TODO: This class will not be needed once 
 // https://github.com/GoogleCloudPlatform/appengine-pipelines/issues/3 is fixed.
public class ExamineStatusAndReturnResult<R> extends Job1<MapReduceResult<R>, ResultAndStatus<R>> {

  private static final long serialVersionUID = -4916783324594785878L;

  private final String stage;

  public ExamineStatusAndReturnResult(String stage) {
    this.stage = stage;
  }

  @Override
  public Value<MapReduceResult<R>> run(ResultAndStatus<R> resultAndStatus) {
    Status status = resultAndStatus.getStatus();
    if (status.getStatusCode() == Status.StatusCode.DONE) {
      return immediate(resultAndStatus.getResult());
    }
    throw new MapReduceJobException(stage, status);
  }
}