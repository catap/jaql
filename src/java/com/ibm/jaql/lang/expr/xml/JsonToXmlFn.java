/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.lang.expr.xml;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.lang.ClassUtils;

import com.ibm.jaql.io.stream.converter.LinesJsonTextOutputStream;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * An expression for converting JSON to XML. It is called as follows:
 * <code>jsonToXml()</code> . It is counterpart of {@link XmlToJsonFn}. But it
 * does not perform a conversion which is reverse to the conversion in
 * {@link XmlToJsonFn}. The reason is:
 * <ol>
 * <li>There is no concepts such as namespace in JSON</li>
 * <li>The conversion is for a conversion from general JSON to XML. It is the
 * commons case that the JSON to be converted is not converted from XML.</li>
 * </ol>
 * 
 * Only a JSON value satisfying the following conditions can be converted to
 * XML:
 * <ol>
 * <li>It is a JSON record whose size is 1.</li>
 * <li>The value of the only JSON pair in this JSON record is not JSON array.</li>
 * </ol>
 * 
 * An array nested in another array does not inherit the nesting array. For
 * example, <code>{content: [[1, 2]]}</code> is converted to:
 * 
 * <pre>
 * &lt;content&gt;
 *   &lt;array&gt;1&lt;/array&gt;
 *   &lt;array&gt;2&lt;/array&gt;
 * &lt;/content&gt;
 * 
 * <pre>
 * @see XmlToJsonFn
 */
public class JsonToXmlFn extends Expr {

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11 {
    public Descriptor() {
      super("jsonToXml", JsonToXmlFn.class);
    }
  }

  public JsonToXmlFn(Expr[] exprs) {
    super(exprs);
  }

  public JsonToXmlFn() {}

  private static final String LINE_SEPARATOR_REGEX = LinesJsonTextOutputStream.LINE_SEPARATOR.replaceAll("\\r", "\\\\r")
                                                                                                .replaceAll("\\n", "\\\\n");
  private static final String XML_DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
  private static final String INDENT_UNIT = "  ";
  private static final JsonString ARRAY = new JsonString("array");

  private boolean firstLine;

  // For XML pretty print
  private boolean seenText;
  private boolean seenTagEnd;
  private int indentCount;

  @Override
  public JsonValue eval(Context context) throws Exception {
    JsonValue jv = exprs[0].eval(context);
    return toJsonArray(jv);
  }

  /**
   * Converts the JSON value into a JSON array for a XML file.
   * 
   * @param jv JSON value
   * @return JSON array for a XML file
   * @see #toXml(JsonValue)
   */
  public JsonArray toJsonArray(JsonValue jv) {
    firstLine = true;
    seenText = false;
    seenTagEnd = true;
    indentCount = 0;

    String xml = toXml(jv);
    String[] lines = xml.split(LINE_SEPARATOR_REGEX);
    int len = lines.length;
    BufferedJsonArray ja = new BufferedJsonArray(len + 1);
    ja.set(0, new JsonString(XML_DECL));
    for (int i = 0; i < len; i++) {
      ja.set(i + 1, new JsonString(lines[i]));
    }
    return ja;
  }

  /**
   * Returns the XML string for the JSON value.
   * 
   * @param v JSON value
   * @return XML string
   * @throws IllegalArgumentException If the JSON value can be converted to XML.
   * @throws Exception
   */
  String toXml(JsonValue v) {
    String message = "Only if a JSON value satisfies the following conditions, "
        + "it can be converted to XML.\n"
        + "  1. It must be a JSON record whose size is 1.\n"
        + "  2. The value of the only JSON pair in this JSON record can't be JSON array.";
    if (!(v instanceof JsonRecord))
      throw new IllegalArgumentException("'" + v + "' is a "
          + ClassUtils.getShortClassName(v.getClass()) + " not a JSON record. "
          + message);
    JsonRecord r = (JsonRecord) v;
    if (r.size() != 1)
      throw new IllegalArgumentException("'" + v + "''s size is not 1. "
          + message);
    Iterator<Entry<JsonString, JsonValue>> it = r.iteratorSorted();
    Entry<JsonString, JsonValue> e = it.next();
    JsonValue value = e.getValue();
    if (value instanceof JsonArray)
      throw new IllegalArgumentException("The value of the only JSON pair '"
          + v + "' is a JSON Array. " + message);
    return value(e.getKey(), value);
  }

  /**
   * Returns the XML string for the field name and JSON value.
   * 
   * @param fn The field name
   * @param jv JSON value
   * @return XML string
   */
  private String value(JsonString fn, JsonValue jv) {
    if (jv instanceof JsonRecord) {
      return record(fn, (JsonRecord) jv);
    } else if (jv instanceof JsonArray) {
      return array(fn, (JsonArray) jv);
    } else {
      return atom(fn, (JsonAtom) jv);
    }
  }

  /**
   * Returns the XML string for the field name and JSON record.
   * 
   * @param fn The field name
   * @param jr JSON record
   * @return XML string
   */
  String record(JsonString fn, JsonRecord jr) {
    StringBuilder b = new StringBuilder();
    String tagName = fn == null ? null : fn.toString();
    if (tagName != null) {
      b.append(tagStart(tagName));
    }
    Iterator<Entry<JsonString, JsonValue>> it = jr.iteratorSorted();
    while (it.hasNext()) {
      Entry<JsonString, JsonValue> e = it.next();
      JsonString k = e.getKey();
      JsonValue v = e.getValue();
      if (v instanceof JsonArray) {
        b.append(array(k, (JsonArray) v));
      } else if (v.equals("")) {
        b.append('<');
        b.append(k);
        b.append("/>");
      } else {
        b.append(value(k, v));
      }
    }
    if (tagName != null) {
      b.append(tagEnd(tagName));
    }
    return b.toString();
  }

  /**
   * Return the XML string for the field name and JSON array.
   * 
   * @param fn The field name
   * @param ja Json array
   * @return XML string
   */
  String array(JsonString fn, JsonArray ja) {
    StringBuilder sb = new StringBuilder();
    /*
     * If the field name is null, array is used as tag name.
     */
    JsonString tagName = fn == null ? ARRAY : fn;
    for (JsonValue jv : ja) {
      if (jv instanceof JsonArray) {
        String tn = tagName.toString();
        sb.append(tagStart(tn));
        /*
         * For nested array (an array is contained in another arrary), array is
         * used as tag name.
         */
        sb.append(array(null, (JsonArray) jv));
        sb.append(tagEnd(tn));
      } else
        sb.append(value(tagName, jv));
    }
    return sb.toString();
  }

  /**
   * Returns the XML string for the field name and JSON atom.
   * 
   * @param fn The field name
   * @param atom JSON atom
   * @return XML string
   * @throws NullPointerException If the field name is <code>null</code>
   */
  String atom(JsonString fn, JsonAtom atom) {
    assert fn != null : "Field name is null";
    String s = escape(atom);
    String tagName = fn.toString();
    if (s.trim().length() == 0)
      return emptyNode(tagName);
    else
      return node(tagName, s);
  }

  /**
   * Return the node string. For XML pretty print, <code>seenText</code> is set
   * to <code>true</true>.
   * 
   * @param name A node name
   * @param text The node text
   * @return Node string
   */
  private String node(String name, String text) {
    StringBuilder sb = new StringBuilder();
    sb.append(tagStart(name));
    seenText = true;
    sb.append(text);
    sb.append(tagEnd(name));
    return sb.toString();
  }

  private String emptyNode(String name) {
    return "<" + name + "/>";
  }

  /**
   * Returns the tag start string. The following steps are performed for XML
   * pretty print:
   * <ol>
   * <li>If <code>seenTagEnd</code> is <code>true</code>, increase indention.</li>
   * <li>
   * <code>seenTagEnd</code> is set to <code>false</code> in this method.</li>
   * </ol>
   * 
   * @param name A tag name
   * @return Tag start string
   */
  private String tagStart(String name) {
    StringBuilder sb = new StringBuilder();
    sb.append(getLineSeparator());
    if (!seenTagEnd) {
      indentCount++;
    }
    seenTagEnd = false;
    indent(sb);
    sb.append("<" + name + ">");
    return sb.toString();
  }

  /**
   * Returns the tag end string. The following steps are performed for XML
   * pretty print:
   * <ol>
   * <li><code>seenTagEnd</code> is set to <code>true</code>.</li>
   * <li>If <code>seenText</code> is <code>false</code>, decrease indention and
   * the end tag is added in a new line. Otherwise, set <code>seenText</code> to
   * <code>false</code>.</li>
   * </ol>
   * 
   * @param name A tage name
   * @return Tage end string
   */
  private String tagEnd(String name) {
    seenTagEnd = true;
    StringBuilder sb = new StringBuilder();
    if (!seenText) {
      indentCount--;
      sb.append(getLineSeparator());
      indent(sb);
    } else {
      seenText = false;
    }
    sb.append("</" + name + ">");
    return sb.toString();
  }

  /*
   * Adds the indent string.
   */
  private void indent(StringBuilder sb) {
    sb.append(getIndentString());
  }

  /*
   * Returns the current indent string.
   */
  private String getIndentString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indentCount; i++)
      sb.append(INDENT_UNIT);
    return sb.toString();
  }

  /*
   * Esacpes the string representation of a JSON atom.
   */
  private String escape(JsonAtom atom) {
    if (atom == null)
      return "";
    String str = atom.toString();
    if (str.length() == 0)
      return "";
    return escape(str);
  }

  private String getLineSeparator() {
    if (!firstLine) {
      return LinesJsonTextOutputStream.LINE_SEPARATOR;
    } else {
      firstLine = false;
      return "";
    }
  }

  /**
   * Replaces special characters with XML escapes:
   * 
   * <ol>
   * <li>&amp; (ampersand) is replaced by &amp;amp;</li>
   * <li>&lt; (less than) is replaced by &amp;lt;</li>
   * <li>&gt; (greater than) is replaced by &amp;gt;</li>
   * <li>&quot; (double quote) is replaced by &amp;quot;</li>
   * </ol>
   * 
   * @param string The string to be escaped.
   * @return The escaped string.
   */
  private String escape(String string) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0, len = string.length(); i < len; i++) {
      char c = string.charAt(i);
      switch (c) {
      case '&':
        sb.append("&amp;");
        break;
      case '<':
        sb.append("&lt;");
        break;
      case '>':
        sb.append("&gt;");
        break;
      case '"':
        sb.append("&quot;");
        break;
      default:
        sb.append(c);
      }
    }
    return sb.toString();
  }
}
