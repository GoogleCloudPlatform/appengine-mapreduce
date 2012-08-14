// Copyright 2012 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.InputReader;
import com.google.appengine.tools.mapreduce.MapReduceSettings;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.OutputWriter;
import com.google.appengine.tools.pipeline.Job;
import com.google.appengine.tools.pipeline.JobSetting;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.List;

/**
 *
 * @author ohler@google.com (Christian Ohler)
 */
public class Util {

  private Util() {}

  public static JobSetting[] jobSettings(MapReduceSettings settings, JobSetting... moreSettings) {
    ImmutableList.Builder<JobSetting> b = ImmutableList.builder();
    b.add(Job.onBackend(settings.getBackend()));
    for (JobSetting s : moreSettings) {
      b.add(s);
    }
    return b.build().toArray(new JobSetting[0]);
  }

  public static <I> List<? extends InputReader<I>> createReaders(Input<I> input) {
    try {
      return input.createReaders();
    } catch (IOException e) {
      throw new RuntimeException(input + ".createReaders() threw IOException");
    }
  }

  public static <O> List<? extends OutputWriter<O>> createWriters(Output<O, ?> output) {
    try {
      return output.createWriters();
    } catch (IOException e) {
      throw new RuntimeException(output + ".createWriters() threw IOException");
    }
  }

}
