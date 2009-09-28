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
package com.ibm.jaql.lang.expr.csv;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ibm.jaql.AbstractLoggableTest;
import com.ibm.jaql.json.parser.JsonParserUtil;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

public class JsonToCsvFnTest extends AbstractLoggableTest {

  private JsonToCsvFn fn = new JsonToCsvFn();

  @Test
  public void array() throws Exception {

    verifyArray("1,2,3", "[1, 2, 3]", null, null);
    verifyArray("\"aaa\",\"bbb\",\"ccc\"", "['aaa', 'bbb', 'ccc']", null, null);

    // delimiter
    verifyArray("\"aaa\":\"bbb\":\"ccc\"", "['aaa', 'bbb', 'ccc']", null, ":");

    // table-like --------------------------------------------------------------

    // array row with the same length
    verifyArray("1,2" + "\n10,20" + "\n100,200",
                "[[1, 2], [10, 20], [100, 200]]",
                null,
                null);
    verifyArray("1=2" + "\n10=20" + "\n100=200",
                "[[1, 2], [10, 20], [100, 200]]",
                null,
                "=");
    verifyArray("11,12" + "\n21,22",
                "[{col1: 11, col2: 12}, {col1: 21, col2: 22}]",
                "['col1', 'col2']",
                null);

    // array row with different lengths
    verifyArray("1,2" + "\n3,4,5", "[[1,2], [3, 4, 5]]", null, null);
    verifyArray("119,," + "\n,1,2",
                "[{name: 119}, {a: 1, b: 2}]",
                "['name', 'a', 'b']",
                null);

    // mixed array row and record row
    verifyArray("1,2" + "\n" + "\n11,22",
                "[[1, 2], {co1: 100, col2: 200}, [11, 22]]",
                null,
                null);
    verifyArray("1,2" + "\n100,200",
                "[[1, 2], {col1: 100, col2: 200}]",
                "['col1', 'col2']",
                null);

    verifyArray("11,22" + "\n1,2,3,4",
                "[{col1: 11, col2: 22}, [1, 2, 3, 4]]",
                "['col1', 'col2']",
                null);
  }

  @Test
  public void record() throws Exception {
    verifyRecord("\"lisp\",5,\"yao\"",
                 "{name: 'yao', lang: 'lisp', level: 5}",
                 null);
    verifyRecord("\"lisp\"<->5<->\"yao\"",
                 "{name: 'yao', lang: 'lisp', level: 5}",
                 "<->");
    debug(record("{list: [1, 2, 3], name: 'STM'}", null));
  }

  @Test
  public void atom() throws Exception {
    assertEquals("1", fn.atom(new JsonLong(1)));
    assertEquals("\"name\"", fn.atom(new JsonString("name")));
    assertEquals("\"1970-01-01T00:00:00Z\"", fn.atom(new JsonDate(0)));
  }

  private void verifyArray(String expected,
                           String valStr,
                           String feildsStr,
                           String del) throws Exception {
    String csv = array(valStr, feildsStr, del);
    assertEquals(expected, csv);
  }

  private String array(String valStr, String feildsStr, String del) throws Exception {
    JsonArray ja = (JsonArray) JsonParserUtil.parse(valStr);
    String csv = fn.array(ja, feildsStr == null ? null
        : (JsonArray) JsonParserUtil.parse(feildsStr), toJsonString(del));
    return csv;
  }

  private void verifyRecord(String expected, String jsonStr, String del) throws Exception {
    String csv = record(jsonStr, del);
    assertEquals(expected, csv);
  }

  private String record(String str, String del) throws Exception {
    JsonRecord jr = (JsonRecord) JsonParserUtil.parse(str);
    return fn.record(jr, toJsonString(del));
  }
}
