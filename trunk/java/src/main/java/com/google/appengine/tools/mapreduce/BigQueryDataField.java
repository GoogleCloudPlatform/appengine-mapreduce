package com.google.appengine.tools.mapreduce;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to provide additional information related to BigQuery data fields. @see
 * TableFieldSchema.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface BigQueryDataField {
  /**
   * Description of the BigQuery field
   */
  String description() default "";

  /**
   * The name of the bigquery column. By default it is same as the name of the field in the class.
   * Use this annotation to provide a different name.
   */
  String name() default "";

  /**
   * Mode of a bigquery table column determines whether it is repeated, required or nullable. A
   * required column must not be left null or empty which loading data. By default it is nullable.
   */
  BigQueryFieldMode mode() default BigQueryFieldMode.NULLABLE;
}
