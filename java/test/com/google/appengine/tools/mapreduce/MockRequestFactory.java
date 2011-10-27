// Copyright 2011 Google Inc. All Rights Reserved.
package com.google.appengine.tools.mapreduce;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;

import org.apache.hadoop.mapreduce.JobID;

import javax.servlet.http.HttpServletRequest;

/**
 */
public class MockRequestFactory {

  private MockRequestFactory() {
  }

  public static HttpServletRequest createMockMapReduceRequest(JobID jobId) {
    return createMockMapReduceRequest(jobId, "default", 0);
  }

  public static HttpServletRequest createMockMapReduceRequest(JobID jobId, String queueName,
      int sliceNumber) {
    HttpServletRequest request = createMock(HttpServletRequest.class);
    expect(request.getParameter(AppEngineJobContext.JOB_ID_PARAMETER_NAME))
        .andReturn(jobId.toString())
        .anyTimes();
    expect(request.getParameter(AppEngineJobContext.SLICE_NUMBER_PARAMETER_NAME))
        .andReturn(String.valueOf(sliceNumber))
        .anyTimes();
    expect(request.getHeader(AppEngineJobContext.QUEUE_NAME_HEADER))
        .andReturn(queueName)
        .anyTimes();
    return request;
  }
}
