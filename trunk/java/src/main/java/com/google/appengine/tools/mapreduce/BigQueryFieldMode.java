package com.google.appengine.tools.mapreduce;

/**
 * The supported bigquery field modes.
 */
public enum BigQueryFieldMode {

  REPEATED("repeated"), NULLABLE("nullable"), REQUIRED("required");

  private final String value;

  private BigQueryFieldMode(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
