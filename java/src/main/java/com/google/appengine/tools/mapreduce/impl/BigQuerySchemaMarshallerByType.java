package com.google.appengine.tools.mapreduce.impl;

import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.findMarshaller;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldDescription;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldMode;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldName;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.getFieldsToSerialize;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.isCollectionOrArray;
import static com.google.appengine.tools.mapreduce.impl.BigQueryFieldUtil.validateTypeForSchemaMarshalling;
import static com.google.appengine.tools.mapreduce.impl.util.BigQueryDataTypeUtil.getBigQueryType;
import static com.google.appengine.tools.mapreduce.impl.util.BigQueryDataTypeUtil.isSimpleBigQueryType;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.appengine.tools.mapreduce.BigQueryDataField;
import com.google.appengine.tools.mapreduce.BigQueryFieldMode;
import com.google.appengine.tools.mapreduce.BigQueryIgnore;
import com.google.appengine.tools.mapreduce.impl.util.BigQueryDataTypeUtil;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validates and generates {@link TableSchema} for type T by using reflection and parsing its
 * annotations. {@link Field}s in class T should can be one of the following simple types or a
 * combination of these : String, StringBuilder, StringBuffer, Character, char, Integer, int,
 * BigInteger, Short, short, Long, Float, float, Double, double, BigDecimal, Boolean, boolean,
 * Currency, Date, URI, Path. It maintains {@link BigqueryFieldMarshaller}s for each of the above
 * types and if a field is of any other type the user can provide {@link BigqueryFieldMarshaller}
 * for it. The code first looks for marshaller in the map of field to
 * {@link BigqueryFieldMarshaller} provided and then looks up in the internal list as mentioned. If
 * no marshaller is found it recursively tries to resolve it following the same process.
 * Additionally T should not be a parameterized type, an interface or an abstract class as these
 * types cannot be marshalled into a consistent {@link TableSchema}.
 *
 * @param <T> type of class it can marshal into {@link TableSchema}.
 */
final class BigQuerySchemaMarshallerByType<T> implements Serializable {

  private static final long serialVersionUID = -775569328952911303L;
  private final Class<T> type;
  private final Map<Field, BigqueryFieldMarshaller> marshallers;
  private final transient IdentityHashMap<Class<?>, Field> typeToField;

  /**
   * @param type of the object to be marshalled by this marshaller.
   * @param marshallers map of field to {@link BigqueryFieldMarshaller} for fields that are
   *        not/cannot be resolved to one of the following types : String, StringBuilder,
   *        StringBuffer, Character, char, Integer, int, BigInteger, Short, short, Long, Float,
   *        float, Double, double, BigDecimal, Boolean, boolean, Currency, Date, URI, Path.
   */
  public BigQuerySchemaMarshallerByType(Class<T> type,
      Map<Field, BigqueryFieldMarshaller> marshallers) {
    this.type = type;
    this.marshallers = marshallers;
    this.typeToField = new IdentityHashMap<>();
    validateType(type, marshallers);
  }

  /**
   * Returns a list of bigquery {@link TableFieldSchema} corresponding to each {@link Field} in the
   * class. It uses annotations ({@link BigQueryIgnore} and {@link BigQueryDataField}) and
   * reflection to infer information needed to define the fields in the way bigquery expects.
   */
  private List<TableFieldSchema> getSchema(Class<?> type) {
    List<TableFieldSchema> toRet = new ArrayList<>();
    Set<Field> fieldsToMap = getFieldsToSerialize(type);
    for (Field field : fieldsToMap) {
      BigqueryFieldMarshaller marshaller = findMarshaller(field, marshallers);
      if (marshaller != null) {
        toRet.add(getFieldSchema(field, marshaller.getSchemaType()));
      } else if (isCollectionOrArray(field.getType())) {
        toRet.add(getSchemaOfRepeatedFields(field));
      } else {
        toRet.add(getFieldSchema(field, field.getType()));
      }
    }
    return toRet;
  }

  private TableFieldSchema getFieldSchema(Field field, Class<?> type) {
    TableFieldSchema toReturn = parseFieldAnnotations(field);
    if (isSimpleBigQueryType(type)) {
      return toReturn.setType(getBigQueryType(type));
    }
    toReturn.setType(BigQueryConstants.RECORD_TYPE);
    return toReturn.setFields(getSchema(type));
  }

  /**
   * Generates {@link TableFieldSchema} for fields which are of repeated type like collection or
   * array. It recursively generates schema for the parameter type of the collection or array.
   */
  private TableFieldSchema getSchemaOfRepeatedFields(Field field) {
    Class<?> parameterType = BigQueryFieldUtil.getParameterTypeOfRepeatedField(field);
    TableFieldSchema tf = parseFieldAnnotations(field);
    tf.setMode(BigQueryFieldMode.REPEATED.getValue());
    if (BigqueryFieldMarshallers.getMarshaller(parameterType) != null) {
      return tf.setType(BigQueryDataTypeUtil.getBigQueryType(
          BigqueryFieldMarshallers.getMarshaller(parameterType).getSchemaType()));
    }
    tf.setType(BigQueryConstants.RECORD_TYPE);
    return tf.setFields(getSchema(parameterType));
  }

  /**
   * Parses the annotations on field and returns a populated {@link TableFieldSchema}.
   */
  private TableFieldSchema parseFieldAnnotations(Field field) {
    TableFieldSchema tf = new TableFieldSchema();
    String name = getFieldName(field);
    String desc = getFieldDescription(field);
    String fieldMode = getFieldMode(field);

    return tf.setName(name).setDescription(desc).setMode(fieldMode);
  }

  /**
   * Validates that the type can be marshalled by using using the provided marshallers and the
   * internal marshallers to determine whether {@link TableSchema} can be generated for the given
   * type.
   *
   * @param type to validate.
   * @param marshallers map of field to {@link BigqueryFieldMarshaller} for unsupported fields.
   */
  private void validateType(Class<?> type, Map<Field, BigqueryFieldMarshaller> marshallers) {
    if (typeToField.containsKey(type)) {
      throw new IllegalArgumentException(this.type
          + " contains cyclic reference for the field with type "
          + type + ". Hence cannot be resolved into bigquery schema.");
    }
    try {
      typeToField.put(type, null);
      validateTypeForSchemaMarshalling(type);
      Set<Field> fieldsToMap = getFieldsToSerialize(type);
      for (Field field : fieldsToMap) {
        if ((marshallers == null || !marshallers.containsKey(field))
            && BigqueryFieldMarshallers.getMarshaller(field.getType()) == null) {
          validateType(field.getType(), marshallers);
        }
      }
    } finally {
      typeToField.remove(type);
    }
  }

  /**
   * Returns the {@link TableSchema} for the parameter type of this class.
   */
  public TableSchema getSchema() {
    return new TableSchema().setFields(getSchema(type));
  }
}
