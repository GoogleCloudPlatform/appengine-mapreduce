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

import org.apache.hadoop.conf.Configuration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.AccessControlException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * An XML file containing a list of Configuration templates, with user visible
 * names. The format is:
 * <pre>
 * <configurations>
 * <configuration name="Foo">...</configuration>
 * <configuration name="Bar">...</configuration>
 * ...
 * </configurations>
 * </pre>
 *
 * <p>where each of the configuration entries is a configuration template as
 * described in {@link ConfigurationTemplatePreprocessor}, with a human readable
 * name in the {@code name} attribute.
 *
 */
public class MapReduceXml {
  private Map<String, Element> nameToConfigMap = new TreeMap<String, Element>();
  private Document doc;

  /**
   * Returns an InputStream for the contents of mapreduce.xml, which should be
   * located at the path WEB-INF/mapreduce.xml in the application root directory.
   */
  private static InputStream getMapReduceXmlInputStream() throws FileNotFoundException {
    String path;
    try {
      // Gets the location of the containing JAR
      URL codeSourceUrl = MapReduceXml.class.getProtectionDomain().getCodeSource().getLocation();
      path = URLDecoder.decode(codeSourceUrl.getPath(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new FileNotFoundException("Couldn't find mapreduce.xml");
    }

    File currentDirectory = new File(path);
    // If we're encountering errors on the way up and can't find the file, we'd
    // like to propagate the error upwards.
    Throwable lastError = null;

    // Walk up the tree, looking for a WEB-INF/mapreduce.xml . We only expect
    // this file to exist in the root dir, but I can't figure out a good way
    // to find the root dir.
    while (currentDirectory != null) {
      File mapReduceFile = new File(currentDirectory.getPath() + File.separator
          + "mapreduce.xml");
      try {
        if (mapReduceFile.exists()) {
          return new FileInputStream(mapReduceFile);
        }
      // We'll add exceptions as they're encountered. We don't want to catch
      // (for instance) DeadlineExceededException.
      } catch (AccessControlException e) {
        lastError = e;
      }
      currentDirectory = currentDirectory.getParentFile();
    }

    if (lastError != null) {
      // This would be nicer if FileNotFoundException took a throwable to wrap or if I had been
      // smarter and declared this method to throw IOException. C'est la vie.
      throw new FileNotFoundException(
          "Couldn't find mapreduce.xml. Additionally, encountered " + lastError.toString()
          + " during processing.");
    }

    throw new FileNotFoundException("Couldn't find mapreduce.xml");
  }

  /**
   * Factory method for creating a MapReduceXml from the WEB-INF/mapreduce.xml file.
   */
  public static MapReduceXml getMapReduceXmlFromFile() throws FileNotFoundException {
    return new MapReduceXml(getMapReduceXmlInputStream());
  }

  /**
   * Initializes MapReduceXml with the contents of the given input stream,
   * which should contain the contents of a mapreduce.xml file.
   */
  public MapReduceXml(InputStream xmlInputStream) {
    // TODO(user): Refactor out DOM ugliness into utility class.
    DocumentBuilderFactory docBuilderFactory =
        ConfigurationTemplatePreprocessor.createConfigurationDocBuilderFactory();
    try {
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(xmlInputStream);
      Element root = doc.getDocumentElement();
      if (!"configurations".equals(root.getTagName())) {
        throw new RuntimeException(
            "Bad configuration list file: top-level element not <configurations>");
      }

      NodeList configurations = root.getChildNodes();

      for (int i = 0; i < configurations.getLength(); i++) {
        if (!(configurations.item(i) instanceof Element)) {
          continue;
        }
        Element configElement = (Element) configurations.item(i);
        String name = configElement.getAttribute("name");
        if (!name.equals("")) {
          nameToConfigMap.put(name, configElement);
        }
      }
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Couldn't create XML parser.", e);
    } catch (UnsupportedEncodingException e) {
      throw new InvalidConfigurationException("Couldn't parse mapreduce.xml", e);
    } catch (SAXException e) {
      throw new InvalidConfigurationException("Couldn't parse mapreduce.xml", e);
    } catch (IOException e) {
      throw new RuntimeException("Couldn't read mapreduce.xml", e);
    }
  }

  private static boolean tryTransformerFactoryGuess(String guess) {
    try {
      Class.forName(guess);
      System.setProperty("javax.xml.transform.TransformerFactory", guess);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static boolean guessAttempted = false;

  /**
   * Some JDKs apparently don't set the TransformerFactory setting to a class
   * that actually exists, so probe a couple of possible values here.
   */
  private synchronized static void guessAndSetTransformerFactoryProp() {
    if (guessAttempted) {
      return;
    }
    guessAttempted = true;
    if (tryTransformerFactoryGuess("org.apache.xalan.processor.TransformerFactoryImpl")) {
      return;
    }
    if (tryTransformerFactoryGuess(
        "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl")) {
      return;
    }
    // If we didn't find any, just continue and hope the JDK does the right
    // thing by default.
  }

  /**
   * Returns the template named by {@code name} as an xml string.
   */
  public String getTemplateAsXmlString(String name) {
    guessAndSetTransformerFactoryProp();
    Element templateElement = nameToConfigMap.get(name);

    if (templateElement == null) {
      throw new IllegalArgumentException("Couldn't find configuration with name " + name);
    }

    DOMSource source = new DOMSource(templateElement);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StreamResult result = new StreamResult(baos);
    TransformerFactory factory = TransformerFactory.newInstance();
    try {
      Transformer transformer = factory.newTransformer();
      transformer.transform(source, result);
      return baos.toString("UTF8");
    } catch (TransformerConfigurationException e) {
      throw new RuntimeException("Error generating templated XML", e);
    } catch (TransformerException e) {
      throw new RuntimeException("Error generating templated XML", e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("JDK doesn't support UTF8", e);
    }
  }

  /**
   * Returns the set of configuration names defined in this object.
   */
  public Set<String> getConfigurationNames() {
    return nameToConfigMap.keySet();
  }

  /**
   * Returns a configuration based on the template in this MapReduceXml
   * identified by {@code name}, substituting in the template parameters
   * from {@code params}.
   */
  public Configuration instantiateConfiguration(
      String name, Map<String, String> params)
  throws FileNotFoundException {
    // TODO(user): Add error handling for the template parameter exceptions.
    String template = getTemplateAsXmlString(name);
    ConfigurationTemplatePreprocessor preprocessor =
      new ConfigurationTemplatePreprocessor(template);
    String configurationString = preprocessor.preprocess(params);
    Configuration configuration =
      ConfigurationXmlUtil.getConfigurationFromXml(configurationString);
    return configuration;
  }
}
