package com.google.appengine.tools.mapreduce.impl;

import com.google.appengine.tools.mapreduce.Input;
import com.google.appengine.tools.mapreduce.MapOnlyMapper;
import com.google.appengine.tools.mapreduce.MapReduceSpecification;
import com.google.appengine.tools.mapreduce.MapSpecification;
import com.google.appengine.tools.mapreduce.Mapper;
import com.google.appengine.tools.mapreduce.Marshaller;
import com.google.appengine.tools.mapreduce.Output;
import com.google.appengine.tools.mapreduce.Reducer;
import com.google.common.base.Preconditions;

import java.lang.reflect.Method;

final class InProcessUtil {

  private static Method getMethod(Class<?> clazz, String name) {
    if (clazz == null) {
      throw new RuntimeException("Could not find method " + name);
    }
    try {
      Method method = clazz.getDeclaredMethod(name);
      method.setAccessible(true);
      return method;
    } catch (NoSuchMethodException e) {
      return getMethod(clazz.getSuperclass(), name);
    }
  }

  private static <T> T invoke(Object spec) {
    Preconditions.checkNotNull(spec, "Null specification");
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    String methodName = stackTraceElements[2].getMethodName();
    Method method = getMethod(spec.getClass(), methodName);
    try {
      method.setAccessible(true);
      return (T) method.invoke(spec);
    } catch (Exception e) {
      throw new RuntimeException("Failed to convert spec to map: " + spec, e);
    }
  }

  @SuppressWarnings("rawtypes")
  static Input getInput(MapSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Input getInput(MapReduceSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Output getOutput(MapSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Output getOutput(MapReduceSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static MapOnlyMapper getMapper(MapSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Mapper getMapper(MapReduceSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Marshaller getKeyMarshaller(MapReduceSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Reducer getReducer(MapReduceSpecification spec) {
    return invoke(spec);
  }

  @SuppressWarnings("rawtypes")
  static Integer getNumReducers(MapReduceSpecification spec) {
    return invoke(spec);
  }
}
