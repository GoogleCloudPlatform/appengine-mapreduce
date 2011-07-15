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

package com.google.appengine.tools.mapreduce;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Utility functions for serializing datastore keys.
 * Package visible to minimize exposed API.
 *
 *
 */
class DatastoreSerializationUtil {
  private DatastoreSerializationUtil() {
  }

  /**
   * Serializes a datastore key, gracefully handling nulls.
   *
   * @param o the DataOutput to serialize to
   * @param key the datastore key to serialize
   * @throws IOException if the serialization goes horribly awry
   */
  public static void writeKeyOrNull(DataOutput o, Key key) throws IOException {
    if (key == null) {
      o.writeUTF("null");
    } else {
      o.writeUTF(KeyFactory.keyToString(key));
    }
  }

  /**
   * Deserializes a datastore key, gracefully handling nulls.
   *
   * @param i the DataInput to deserialize from
   * @return the deserialized datastore key or {@code null}
   * @throws IOException if the deserialization goes horribly awry
   */
  public static Key readKeyOrNull(DataInput i) throws IOException {
    String keyString = i.readUTF();
    if (keyString.equals("null")) {
      return null;
    }
    return KeyFactory.stringToKey(keyString);
  }
}
