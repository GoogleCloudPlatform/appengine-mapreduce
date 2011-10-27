/*
 * Copyright 2010 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.mapreduce.v2.impl;

import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Internal util class for serializing/deserializing Writables.
 *
 *
 */
public class Writables {
  private Writables() {
  }

  /**
   * Initializes the writable via readFields using the data encoded in s.
   *
   * @param s string containing the fields to initialize w
   * @param w writable to be initialized
   */
  public static void initializeWritableFromString(String s, Writable w) {
    try {
      initializeWritableFromByteArray(s.getBytes("UTF8"), w);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("Your JDK doesn't support UTF8. Interesting.", e);
    }

  }

  /**
   * Initializes the writable via readFields using the data encoded in b.
   *
   * @param b byte array containing the fields to initialize w with
   * @param w writable to be initialized
   */
  public static void initializeWritableFromByteArray(byte[] b, Writable w) {
    try {
      w.readFields(new DataInputStream(new ByteArrayInputStream(b)));
    } catch (IOException ioe) {
      throw new RuntimeException(
          "Got an impossible IOException from a ByteArrayInputStream.", ioe);
    }
  }

  /**
   * Returns the encoded version of a writable as a String.
   *
   * @param w writable to encode
   * @return the encoded String version of the Writable
   */
  public static String createStringFromWritable(Writable w) {
    try {
      return new String(createByteArrayFromWritable(w), "UTF8");
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("Your JDK doesn't support UTF8. Interesting.", uee);
    }
  }

  public static byte[] createByteArrayFromWritable(Writable w) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      w.write(dos);
      dos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(
          "Got an impossible IOException from a ByteArrayOutputStream", e);
    }
  }
}
