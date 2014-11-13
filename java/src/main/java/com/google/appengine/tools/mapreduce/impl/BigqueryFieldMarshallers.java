package com.google.appengine.tools.mapreduce.impl;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.common.collect.ImmutableMap;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.Currency;
import java.util.Date;
import java.util.Map;

/**
 * {@link BigqueryFieldMarshaller}s that can be used to marshal {@link Field}s into
 * {@link TableFieldSchema}.
 */
public class BigqueryFieldMarshallers {
  private BigqueryFieldMarshallers() {
    // utility class
  }

  /**
   * Map of the all the {@link BigqueryFieldMarshaller}s internally used by the
   * {@link BigQueryMarshallerByType} for marshalling types into bigquery load format.
   */
  private static Map<Class<?>, BigqueryFieldMarshaller> marshallers =
      new ImmutableMap.Builder<Class<?>, BigqueryFieldMarshaller>()
          .put(String.class, new BigqueryStringFieldMarshaller())
          .put(StringBuilder.class, new BigqueryStringFieldMarshaller())
          .put(StringBuffer.class, new BigqueryStringFieldMarshaller())
          .put(Character.class, new BigqueryStringFieldMarshaller())
          .put(char.class, new BigqueryStringFieldMarshaller())
          .put(Integer.class, new BigqueryIntegerFieldMarshaller())
          .put(int.class, new BigqueryIntegerFieldMarshaller())
          .put(BigInteger.class, new BigqueryStringFieldMarshaller())
          .put(Short.class, new BigqueryIntegerFieldMarshaller())
          .put(short.class, new BigqueryIntegerFieldMarshaller())
          .put(Long.class, new BigqueryIntegerFieldMarshaller())
          .put(long.class, new BigqueryIntegerFieldMarshaller())
          .put(Float.class, new BigqueryFloatFieldMarshaller())
          .put(float.class, new BigqueryFloatFieldMarshaller())
          .put(Double.class, new BigqueryFloatFieldMarshaller())
          .put(double.class, new BigqueryFloatFieldMarshaller())
          .put(BigDecimal.class, new BigqueryStringFieldMarshaller())
          .put(Boolean.class, new BigqueryBooleanFieldMarshaller())
          .put(boolean.class, new BigqueryBooleanFieldMarshaller())
          .put(Currency.class, new BigqueryStringFieldMarshaller())
          .put(Date.class, new BigqueryDateFieldMarshaller())
          .put(Calendar.class, new BigqueryCalendarFieldMarshaller())
          .put(URI.class, new BigqueryStringFieldMarshaller())
          .put(Path.class, new BigqueryStringFieldMarshaller())
          .build();

  static BigqueryFieldMarshaller getMarshaller(Class<?> type) {
    if (type.isEnum()) {
      return new BigqueryStringFieldMarshaller();
    }
    return marshallers.get(type);
  }

  private static class BigqueryBooleanFieldMarshaller implements BigqueryFieldMarshaller {
    @Override
    public Class<?> getSchemaType() {
      return Boolean.class;
    }

    @Override
    public Object getFieldValue(Field field, Object object) {
      return BigQueryFieldUtil.getFieldValue(field, object);
    }
  }

  private static class BigqueryStringFieldMarshaller implements BigqueryFieldMarshaller {
    @Override
    public Object getFieldValue(Field field, Object object) {
      return BigQueryFieldUtil.getFieldValue(field, object);
    }

    @Override
    public Class<?> getSchemaType() {
      return String.class;
    }
  }
  private static class BigqueryIntegerFieldMarshaller implements BigqueryFieldMarshaller {

    @Override
    public Object getFieldValue(Field field, Object object) {
      return BigQueryFieldUtil.getFieldValue(field, object);
    }

    @Override
    public Class<?> getSchemaType() {
      return Integer.class;
    }
  }
  private static class BigqueryFloatFieldMarshaller implements BigqueryFieldMarshaller {

    @Override
    public Object getFieldValue(Field field, Object object) {
      return BigQueryFieldUtil.getFieldValue(field, object);
    }

    @Override
    public Class<?> getSchemaType() {
      return Float.class;
    }
  }

  private static class BigqueryDateFieldMarshaller implements BigqueryFieldMarshaller {
    @Override
    public Object getFieldValue(Field field, Object object) {
      Date date = (Date) BigQueryFieldUtil.getFieldValue(field, object);
      return date.getTime() / 1000.0;
    }

    @Override
    public Class<?> getSchemaType() {
      return Date.class;
    }
  }

  private static class BigqueryCalendarFieldMarshaller implements BigqueryFieldMarshaller {
    @Override
    public Object getFieldValue(Field field, Object object) {
      Calendar calendar = (Calendar) BigQueryFieldUtil.getFieldValue(field, object);
      return calendar.getTimeInMillis() / 1000.0;
    }

    @Override
    public Class<?> getSchemaType() {
      return Calendar.class;
    }
  }
}
