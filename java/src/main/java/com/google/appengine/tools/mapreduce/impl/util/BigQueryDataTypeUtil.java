package com.google.appengine.tools.mapreduce.impl.util;

import com.google.common.collect.ImmutableMap;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Utility class for converting java types to BigQuery data types
 */
public final class BigQueryDataTypeUtil {

  private static final Map<Class<?>, String> javaTypeToBigQueryType =
      new ImmutableMap.Builder<Class<?>, String>()
          .put(Integer.class, "integer")
          .put(Float.class, "float")
          .put(Boolean.class, "boolean")
          .put(String.class, "string")
          .put(Date.class, "timestamp")
          .put(Calendar.class, "timestamp")
          .build();

  /**
   * @param parameterType java primitive, wrapper or String types
   * @return BigQuery data type
   * */
  public static String getBigQueryType(Class<?> parameterType) {
    return javaTypeToBigQueryType.get(parameterType);
  }

  public static boolean isSimpleBigQueryType(Class<?> parameterType) {
    return javaTypeToBigQueryType.containsKey(parameterType);
  }
}
