package com.google.appengine.tools.mapreduce.impl;

import com.google.api.services.bigquery.model.TableFieldSchema;

import java.lang.reflect.Field;


/**
 * Defines how a {@link Field}s should be interpreted and marshalled while generating its
 * {@link TableFieldSchema} for loading data into bigquery.
 */
public interface BigqueryFieldMarshaller {
  Object getFieldValue(Field field, Object object);

  Class<?> getSchemaType();
}
