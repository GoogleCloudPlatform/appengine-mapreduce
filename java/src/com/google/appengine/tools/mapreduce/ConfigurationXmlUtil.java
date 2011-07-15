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

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Utility class for converting between a Configuration and its XML
 * representation.
 *
 *
 */
public class ConfigurationXmlUtil {
  private ConfigurationXmlUtil() {
  }

  /**
   * Reconstitutes a Configuration from an XML string.
   *
   * @param serializedConf an XML document in Hadoop configuration format
   * @return the Configuration corresponding to the XML
   */
  public static Configuration getConfigurationFromXml(String serializedConf) {
    Preconditions.checkNotNull(serializedConf);

    try {
      byte[] serializedConfBytes = serializedConf.getBytes("UTF8");
      ByteArrayInputStream serializedConfStream = new ByteArrayInputStream(
          serializedConfBytes);

      // false makes Configuration not try to read defaults from the filesystem.
      Configuration conf = new Configuration(false);
      conf.addResource(serializedConfStream);
      return conf;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("JDK doesn't understand UTF8", e);
    }
  }

  /**
   * Serializes a configuration to its corresponding XML document as a string.
   *
   * @param conf the configuration to encode
   * @return the configuration as an XML document
   */
  public static String convertConfigurationToXml(Configuration conf) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      conf.writeXml(baos);
      return baos.toString("UTF8");
    } catch (IOException e) {
      throw new RuntimeException(
          "Got an IOException writing to ByteArrayOutputStream. This should never happen.", e);
    }
  }
}
