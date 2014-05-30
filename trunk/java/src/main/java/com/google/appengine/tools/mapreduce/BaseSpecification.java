// Copyright 2014 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;

import java.io.Serializable;

/**
 * Base specification for MR jobs.
 *
 * @param <I> type of input values
 * @param <O> type of output values
 * @param <R> type of result returned by the {@link Output}
 */
abstract class BaseSpecification<I, O, R> implements Serializable {

  private static final long serialVersionUID = 7750970872420160768L;

  private final String jobName;
  private final Input<I> input;
  private final Output<O, R> output;

  abstract static class Builder<B extends Builder<B, I, O, R>, I, O, R> {

    protected String jobName;
    protected Input<? extends I> input;
    protected Output<? super O, ? extends R> output;

    protected abstract B self();

    /**
     * @param jobName descriptive name for the job (human readable, does not have to be unique).
     */
    public B setJobName(String jobName) {
      this.jobName = jobName;
      return self();
    }

    /**
     * @param input specifies what input the mapper should process
     */
    public B setInput(Input<? extends I> input) {
      this.input = input;
      return self();
    }

    /**
     * @param output specifies what to do with  output values.
     */
    public B setOutput(Output<? super O, ? extends R> output) {
      this.output = output;
      return self();
    }
  }

  @SuppressWarnings("rawtypes")
  BaseSpecification(Builder builder) {
    jobName = Optional.fromNullable(builder.jobName).or("");
    input = checkNotNull(builder.input, "Null input");
    output = checkNotNull(builder.output, "Null output");
  }

  String getJobName() {
    return jobName;
  }

  Input<I> getInput() {
    return input;
  }

  Output<O, R> getOutput() {
    return output;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + jobName + ", " + input + ", " + output + ")";
  }
}
