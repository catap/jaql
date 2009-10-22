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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.ibm.jaql.AbstractTest;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class TestJsonToXmlFn extends AbstractTest {

  private JsonToXmlFn fn = new JsonToXmlFn();

  /**
   * Verifies that for unacceptable JSON values,
   * <code>IllegalArgumentException</code> will be thrown.
   * 
   * @throws Exception
   */
  @Test
  public void invalidJsonToXml() throws Exception {
    verifyInvalidJson("1");
    verifyInvalidJson("\"map\"");
    verifyInvalidJson("[1, 2]");
    verifyInvalidJson("{name: ['a', 'b']}");
  }

  @Test
  public void validJsonToXml() throws Exception {
    verifyValidJson("<name>1</name>", "{name: 1}");
    verifyValidJson("<name><nested>1</nested></name>", "{name: {nested: 1}}");
    verifyValidJson("<name><f1>str1</f1><f2>2</f2></name>",
                    "{name: {f1: 'str1', f2: 2}}");
    verifyValidJson("<name><f1>str1</f1><f2>10</f2><f2>11</f2><f2>12</f2></name>",
                    "{name: {f1: 'str1', f2: [10, 11, 12]}}");
    verifyValidJson("<t><p><array>1</array><array>2</array><array>3</array></p><p><array>a</array><array>b</array><array>c</array></p></t>",
                    "{t: {p: [[1, 2, 3], ['a', 'b', 'c']]}}");
  }

  @Test
  public void jsonRecord() {
    // simple
    verifyRecord("<name>beijing</name>", null, "{name: 'beijing'}");
    verifyRecord("<field><name>beijing</name></field>",
                 "field",
                 "{name: 'beijing'}");
    verifyRecord("<field><name>beijing</name><no>1</no></field>",
                 "field",
                 "{name: 'beijing', no: 1}");

    // nested
    verifyRecord("<desc>1</desc><desc>2</desc>", null, "{desc: [1, 2]}");

    verifyRecord("<out><desc>1</desc><desc>2</desc></out>",
                 "out",
                 "{desc: [1, 2]}");

    verifyRecord("<desc>1</desc><desc>2</desc><type>io</type>",
                 null,
                 "{desc: [1, 2], type: 'io'}");

    verifyRecord("<desc>1</desc><desc>2</desc><meta><type>map</type><value>10</value></meta><type>io</type>",
                 null,
                 "{desc: [1, 2], type: 'io', meta: {type: 'map', value: 10}}");

    // record -> array -> array -> array -> record
    verifyRecord("<content><array><array><age>2</age><name>jaql</name></array><array><city>beijing</city></array></array></content><content>2</content>",
                 null,
                 "{content: [[[{name: 'jaql', age: 2}, {city: 'beijing'}]], 2]}");

  }

  @Test
  public void jsonArray() {
    // without field name
    verifyArray("<array>1</array><array>2</array>", null, "[1, 2]");
    verifyArray("<array>1</array><array/>", null, "[1, null]");

    // with field name
    verifyArray("<lang>lisp</lang><lang>haskell</lang>",
                "lang",
                "['lisp', 'haskell']");

    // nested
    verifyArray("<array><name>beijing</name><no>1</no></array>",
                null,
                "[{name: 'beijing', no: 1}]");
    verifyArray("<field><name>beijing</name><no>1</no></field>",
                "field",
                "[{name: 'beijing', no: 1}]");

    // nested array with field name
    verifyArray("<a><array>1</array><array>2</array></a><a><array>10</array><array>20</array></a><a>100</a>",
                "a",
                "[[1, 2], [10, 20], 100]");

    // nested array without field name
    verifyArray("<array><array>1</array><array>2</array></array><array><array>10</array><array>20</array></array><array>100</array>",
                null,
                "[[1, 2], [10, 20], 100]");
  }

  @Test
  public void validJsonAtom() {
    JsonLong jl = new JsonLong(123);
    assertXmlEquals("<n>123</n>", atomS("n", jl));

    JsonDate jd = new JsonDate(0);
    assertXmlEquals("<date>1970-01-01T00:00:00Z</date>", atomS("date", jd));

    assertXmlEquals("<empty/>", atomS("empty", null));

    JsonString js = new JsonString("jaql programming");
    assertXmlEquals("<name>jaql programming</name>", atomS("name", js));

  }

  private void verifyArray(String expected, String fn, String jrStr) {
    JsonArray sJ = (JsonArray) parse(jrStr);
    assertXmlEquals(expected, arrayS(fn, sJ));
  }

  private void verifyRecord(String expected, String fn, String jrStr) {
    JsonRecord sJ = (JsonRecord) parse(jrStr);
    assertXmlEquals(expected, recordS(fn, sJ));
  }

  private void verifyInvalidJson(String str) throws Exception {
    JsonValue jv = parse(str);
    try {
      fn.toXml(jv);
      fail();
    } catch (IllegalArgumentException iae) {
      debugException(iae);
    }
  }

  private void verifyValidJson(String expectecd, String str) throws Exception {
    JsonValue jv = parse(str);
    String xml = fn.toXml(jv);
    assertXmlEquals(expectecd, xml);
  }

  private void assertXmlEquals(String expected, String actual) {
    actual = actual.replaceAll("\\n *", "");
    assertEquals(expected, actual);
  }

  private String recordS(String s, JsonRecord jr) {
    return fn.record(toJsonString(s), jr);
  }

  private String arrayS(String s, JsonArray ja) {
    return fn.array(toJsonString(s), ja);
  }

  private String atomS(String s, JsonAtom atom) {
    return fn.atom(toJsonString(s), atom);
  }
}