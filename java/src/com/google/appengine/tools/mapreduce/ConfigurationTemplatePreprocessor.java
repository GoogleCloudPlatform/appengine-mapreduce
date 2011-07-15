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

import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * This object preprocesses a Configuration template, filling in template values
 * (those for which the {@code value} XML tag has a {@code template}
 *  XML attribute) using a provided map of names to values.
 *
 * <p>Additionally, when given the template XML, the object can generate a map
 * between map name and user visible name for ease of presentation.
 *
 * <p>As a concrete example, suppose we have the the following XML string:
 *
 * <pre>
 * <property>
 *   <name human="Username to analyze">username</name>
 *   <value template="required" />
 * </property>
 * <property>
 *   <name>basedir</name>
 *   <value>/user/${username}</value>
 * </property>
 * </pre>
 *
 * <p>If we have that XML string stored in xmlString, then calling
 *
 * <pre>
 * Map<String, String> parameters = new Map<String, String>();
 * parameters.put("username", "Bob");
 * new ConfigurationTemplatePreprocessor(xmlString).preprocess(parameters);
 * </pre>
 *
 * <p>returns the string:
 *
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;username&lt;/name&gt;
 *   &lt;value&gt;Bob&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;basedir&lt;/name&gt;
 *   &lt;value&gt;/user/${username}&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 * <p>Hadoop's {@link org.apache.hadoop.conf.Configuration} class will then
 * populate the {@code basedir} property with the value {@code /user/Bob} as
 * usual.
 *
 * <p>You can also enumerate the map from name to user visible name. For
 * instance, given the same starting XML document
 *
 * <pre>
 * new ConfigurationTemplatePreprocessor(xmlString).getHumanNameMap();
 * </pre>
 *
 * <p> returns the {@code Map} corresponding to:
 *
 * <pre>
 * {"username" =&gt; "Username to analyze"}
 * </pre>
 *
 * <p>You can also specify that the child text of a templated value should be
 * used as the default if no value is given by the template="optional"
 * attribute. For instance, if no parameters are given, then:
 *
 * <pre>
 * &lt;property&gt;
 *   &lt;name human="Username to analyze"&gt;username&lt;/name&gt;
 *   &lt;value template="optional"&gt;Bob&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 * <p>evaluates to simply:
 *
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;username&lt;/name&gt;
 *   &lt;value&gt;Bob&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 *
 */
public class ConfigurationTemplatePreprocessor {
  private Document doc;

  /**
   * DDO for the information needed to render the form element for a  single
   * template property.
   */
  public static class TemplateEntryMetadata {
    private String name;
    private String humanName;
    private String defaultValue;

    /**
     *
     * @param humanName Human readable name or the
     * @param defaultValue
     */
    public TemplateEntryMetadata(String name, String humanName, String defaultValue) {
      this.name = name;
      this.humanName = humanName;
      this.defaultValue = defaultValue;
    }

    public String getName() {
      return name;
    }

    public String getHumanName() {
      if (humanName == null) {
        return name;
      }
      return humanName;
    }

    public String getDefaultValue() {
      return defaultValue;
    }

    /**
     * Describes TemplateEntryMetadata as if it were an AbstractMap.
     */
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{");
      if (getDefaultValue() != null) {
        sb.append("defaultValue=");
        sb.append(getDefaultValue());
        sb.append(", ");
      }
      sb.append("humanName=");
      sb.append(getHumanName());
      sb.append(", name=");
      sb.append(getName());
      sb.append("}");
      return sb.toString();
    }
  }

  private Map<String, TemplateEntryMetadata> nameToMetadata =
      new HashMap<String, TemplateEntryMetadata>();
  private Map<String, Element> nameToValueElement = new HashMap<String, Element>();

  private boolean preprocessCalled;

  /**
   * Initializes a ConfigurationTemplatePreprocessor with the template XML
   *
   * @param xmlString the template XML configuration
   */
  public ConfigurationTemplatePreprocessor(String xmlString) {
    DocumentBuilderFactory docBuilderFactory = createConfigurationDocBuilderFactory();
    try {
      DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
      doc = builder.parse(new ByteArrayInputStream(xmlString.getBytes("UTF8")));
      Element root = doc.getDocumentElement();
      if (!"configuration".equals(root.getTagName())) {
        throw new RuntimeException("Bad configuration file: top-level element not <configuration>");
      }

      NodeList props = root.getChildNodes();
      for (int i = 0; i < props.getLength(); i++) {
        Node propNode = props.item(i);
        if (!(propNode instanceof Element)) {
          continue;
        }
        Element prop = (Element) propNode;

        // TODO(user): Currently not implementing nested configuration elements.
        // If there's demand, this can be revisited.

        populateParameterFromProperty(prop);
      }
    } catch (ParserConfigurationException e) {
      throw new RuntimeException("Couldn't create XML parser", e);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("JDK doesn't support UTF8", e);
    } catch (SAXException e) {
      // Would throw a better exception, but Configuration doesn't.
      throw new RuntimeException("Encountered error parsing configuration XML", e);
    } catch (IOException e) {
      throw new RuntimeException("Encountered IOException on a ByteArrayInputStream", e);
    }
  }

  /**
   * For a single {@code Configuration} property, process it, looking
   * for parameter-related attributes, and updating the relevant object maps
   * if found.
   */
  private void populateParameterFromProperty(Element prop) {
    String name = null;
    String humanName = null;
    Element templateValue = null;
    String defaultValue = null;

    NodeList fields = prop.getChildNodes();
    for (int j = 0; j < fields.getLength(); j++) {
      Node fieldNode = fields.item(j);
      if (!(fieldNode instanceof Element)) {
        continue;
      }
      Element field = (Element) fieldNode;
      if ("name".equals(field.getTagName())) {
        if (field.hasAttribute("human")) {
          humanName = field.getAttribute("human");
        }
        name = ((Text) field.getFirstChild()).getData().trim();
      }
      if ("value".equals(field.getTagName()) && field.hasAttribute("template")) {
        templateValue = field;
        if (field.getAttribute("template").equals("optional")) {
          if (field.getFirstChild() != null) {
            defaultValue = field.getFirstChild().getTextContent();
          } else {
            defaultValue = "";
          }
        }
      }
    }

    if (templateValue != null) {
      nameToMetadata.put(name, new TemplateEntryMetadata(name, humanName, defaultValue));
      nameToValueElement.put(name, templateValue);
    }
  }

  /**
   * Creates a DocumentBuilder that mirrors the settings used by Hadoop.
   */
  static DocumentBuilderFactory createConfigurationDocBuilderFactory() {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();

    // Mirroring the settings in org.apache.hadoop.conf.Configuration
    docBuilderFactory.setIgnoringComments(true);
    docBuilderFactory.setNamespaceAware(false);
    try {
      docBuilderFactory.setXIncludeAware(true);
    } catch (UnsupportedOperationException e) {
      // Ignore
    }
    return docBuilderFactory;
  }

  /**
   * Substitutes in the values in params for templated values. After this method
   * is called, any subsequent calls to this method for the same object result in
   * an {@link IllegalStateException}. To preprocess the template again,
   * create another {@code ConfigurationTemplatePreprocessor} from the source XML.
   *
   * @param params a map from key to value of all the template parameters
   * @return the document as an XML string with the template parameters filled
   * in
   * @throws MissingTemplateParameterException if any required parameter is
   * omitted
   * @throws IllegalStateException if this method has previously been called
   * on this object
   */
  public String preprocess(Map<String, String> params) {
    HashMap<String, String> paramsCopy = new HashMap<String, String>(params);
    if (preprocessCalled) {
      throw new IllegalStateException(
          "Preprocess can't be called twice for the same object");
    }
    preprocessCalled = true;

    for (Entry<String, Element> entry : nameToValueElement.entrySet()) {
      Element valueElem = entry.getValue();
      boolean isTemplateValue = valueElem.hasAttribute("template");
      if (paramsCopy.containsKey(entry.getKey()) && isTemplateValue) {
        // Modifies the map in place, but that's fine for our use.
        Text valueData = (Text) valueElem.getFirstChild();
        String newValue = paramsCopy.get(entry.getKey());
        if (valueData != null) {
          valueData.setData(newValue);
        } else {
          valueElem.appendChild(doc.createTextNode(newValue));
        }
        // Remove parameter, so we can tell if they gave us extras.
        paramsCopy.remove(entry.getKey());
      } else if (isTemplateValue) {
        String templateAttribute = valueElem.getAttribute("template");
        if ("required".equals(templateAttribute)) {
          throw new MissingTemplateParameterException(
              "Couldn't find expected parameter " + entry.getKey());
        } else if ("optional".equals(templateAttribute)) {
          // The default value is the one already in the value text, so
          // leave it be. The one exception is if they self-closed the value tag,
          // so go ahead and add a Text child so Configuration doesn't barf.
          if (valueElem.getFirstChild() == null) {
            valueElem.appendChild(doc.createTextNode(""));
          }
        } else {
          throw new IllegalArgumentException(
              "Value " + templateAttribute + " is not a valid template "
              + "attribute. Valid possibilities are: \"required\" or \"optional\".");
        }
        // Remove parameter, so we can tell if they gave us extras.
        paramsCopy.remove(entry.getKey());
      } else {
        throw new IllegalArgumentException("Configuration property " + entry.getKey()
            + " is not a template property");
      }

      // removeAttribute has no effect if the attributes don't exist
      valueElem.removeAttribute("template");
    }

    if (paramsCopy.size() > 0) {
      // TODO(user): Is there a good way to bubble up all bad parameters?
      throw new UnexpectedTemplateParameterException(
          "Parameter " + paramsCopy.keySet().iterator().next()
              + " wasn't found in the configuration template.");
    }

    return getDocAsXmlString();
  }

  // Converts the preprocessor's doc to an XML string and returns it.
  private String getDocAsXmlString() throws TransformerFactoryConfigurationError {
    DOMSource source = new DOMSource(doc);
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
   * Returns a mapping from property name to the TemplateEntryMetadata for each
   * template property.
   */
  public Map<String, TemplateEntryMetadata> getMetadataMap() {
    return nameToMetadata;
  }

  /**
   * Generate a JSON version of the template described by this object named
   * by the given {@code name}.
   */
  public JSONObject toJson(String name) {
    JSONObject propertiesMap = new JSONObject();
    Map<String, TemplateEntryMetadata> metadataMap = getMetadataMap();
    for (Entry<String, TemplateEntryMetadata> entry : metadataMap.entrySet()) {
      convertPropertiesToJson(propertiesMap, entry);
    }

    JSONObject configObject = new JSONObject();
    try {
      configObject.put("name", name);
      configObject.put("mapper_params", propertiesMap);
    } catch (JSONException e) {
      throw new RuntimeException("Hard-coded string is null", e);
    }
    return configObject;
  }

  private void convertPropertiesToJson(JSONObject propertiesMap,
      Entry<String, TemplateEntryMetadata> entry) {
    JSONObject configObject = new JSONObject();
    try {
      configObject.put("human_name", entry.getValue().getHumanName());
      configObject.put("default_value", entry.getValue().getDefaultValue());
    } catch (JSONException e) {
      throw new RuntimeException("Apparently, a string is now null.", e);
    }
    try {
      propertiesMap.put(entry.getKey(), configObject);
    } catch (JSONException e) {
      throw new RuntimeException("Got a null property key");
    }
  }
}
