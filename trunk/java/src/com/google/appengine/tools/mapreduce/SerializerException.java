// Copyright 2011 Google Inc. All Rights Reserved.

package com.google.appengine.tools.mapreduce;

import java.io.IOException;


/**
 * Thrown if a TypeEncoder cannot encode or decode an object.
 *
 */
public class SerializerException extends IOException {

  public SerializerException() {
    super();
  }

  public SerializerException(String reason) {
    super(reason);
  }
}
