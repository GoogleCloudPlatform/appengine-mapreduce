package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.findMarshaller;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldName;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldValue;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldsToSerialize;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getParameterTypeOfRepeatedField;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.isCollection;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.isFieldRequired;
import static com.google.appengine.tools.mapreduce.impl.util.BigQueryDataTypeUtil.isSimpleBigQueryType;

import com.google.appengine.tools.mapreduce.BigQueryFieldMode;
import com.google.appengine.tools.mapreduce.BigQueryIgnore;
import com.google.appengine.tools.mapreduce.impl.util.BigQueryDataTypeUtil;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts objects into Map of field name to field values by using the
 * {@link BigqueryFieldMarshaller}. If a value marshaller for a field is not found then it
 * recursively breaks down that field into simpler types.
 */
final class BigQueryDataMarshallerByType implements Serializable {

  private static final long serialVersionUID = -193029067068464059L;
  private final Map<Field, BigqueryFieldMarshaller> marshallers;

  /**
   * @param marshallers
   */
  public BigQueryDataMarshallerByType(Map<Field, BigqueryFieldMarshaller> marshallers) {
    this.marshallers = marshallers;
  }

  /**
   * Converts the input object into a nested map of field name to field value. Recursively breaks
   * down complex field types to simple bigquery types as listed in {@link BigQueryDataTypeUtil}.
   * Uses reflection to infer the type and value of fields. If any field in the class hierarchy is
   * protected by {@link SecurityManager} the method will fail. Fields annotated with
   * {@link BigQueryIgnore} are ignored
   *
   * @param toMap of type object
   * @return a nested map of field name to field value.
   */
  Map<String, Object> mapFieldNameToValue(Object toMap) {
    if (toMap == null) {
      return null;
    }
    Class<?> typeOfObjectToMap = toMap.getClass();
    Map<String, Object> toRet = new HashMap<>();
    Set<Field> fieldsToMap = getFieldsToSerialize(typeOfObjectToMap);
    Object fieldValue = null;
    for (Field field : fieldsToMap) {
      BigqueryFieldMarshaller marshaller = findMarshaller(field, marshallers);
      if (marshaller != null) {
        fieldValue = marshaller.getFieldValue(field, toMap);
      } else {
        fieldValue = getFieldValue(field, toMap);
      }
      assertFieldValue(field, fieldValue);

      if (marshaller != null) {
        toRet.put(getFieldName(field), fieldValue);
      } else if (BigQueryFieldUtil.isCollectionOrArray(field.getType())) {
        toRet.put(getFieldName(field), mapRepeatedFieldToListOfValues(field, fieldValue));
      } else {
        toRet.put(getFieldName(field), mapFieldNameToValue(fieldValue));
      }
    }
    return toRet;
  }

  /**
   * Asserts that a field annotated as {@link BigQueryFieldMode#REPEATED} is not left null.
   */
  private void assertFieldValue(Field field, Object fieldValue) {
    if (fieldValue == null && isFieldRequired(field)) {
      throw new IllegalArgumentException("Non-nullable field " + field.getName()
          + ". This field is either annotated as REQUIRED or is a primitive type.");
    }
  }

  /**
   * Converts the input object into a nested map of field name to field value. Recursively breaks
   * down complex field types to simple Bigquery types as listed in {@link BigQueryDataTypeUtil}.
   * Uses reflection to infer the type and value of fields. Only accessible fields are marshalled.
   * Fields annotated with {@link BigQueryIgnore} are ignored.
   *
   * @param field {@link Field} to map to list of repeated values
   * @param fieldValue value of the field to resolve
   * @return a nested map of field name to field value.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  List<Object> mapRepeatedFieldToListOfValues(Field field, Object fieldValue) {
    Class<?> fieldType = getParameterTypeOfRepeatedField(field);
    Collection fieldToMap = null;
    if (isCollection(field.getType())) {
      fieldToMap = (Collection) fieldValue;
    } else if (field.getType().isArray()) {
      fieldToMap = Arrays.asList((Object[]) fieldValue);
    }

    List<Object> toRet = Lists.newArrayListWithCapacity(fieldToMap.size());
    if (isSimpleBigQueryType(fieldType)) {
      for (Object o : fieldToMap) {
        toRet.add(o);
      }
    } else {
      for (Object o : fieldToMap) {
        toRet.add(mapFieldNameToValue(o));
      }
    }
    return toRet;
  }
}
