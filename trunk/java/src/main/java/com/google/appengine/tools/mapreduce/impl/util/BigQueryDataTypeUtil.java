package com.google.appengine.tools.mapreduce.impl.util;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Utility class for converting java types to BigQuery data types
 */
public final class BigQueryDataTypeUtil {

  private static final Map<Class<?>, String> javaTypeToBigQueryType =
      new ImmutableMap.Builder<Class<?>, String>()
          .put(Integer.class, "integer")
          .put(int.class, "integer")
          .put(Float.class, "float")
          .put(float.class, "float")
          .put(Double.class, "float")
          .put(double.class, "float")
          .put(Short.class, "integer")
          .put(short.class, "integer")
          .put(Long.class, "integer")
          .put(long.class, "integer")
          .put(Boolean.class, "boolean")
          .put(boolean.class, "boolean")
          .put(Character.class, "string")
          .put(char.class, "boolean")
          .put(String.class, "string")
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
