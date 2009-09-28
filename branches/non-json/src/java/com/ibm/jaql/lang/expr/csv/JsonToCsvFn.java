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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.basic.BasicTextFullSerializer;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonAtom;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.UnquotedJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * A function for converting JSON to CSV. It is called as follows:
 * <code>jsonToCsv(fields, delimiter)</code>. {@link BasicTextFullSerializer} is
 * used to serialize JSON value.
 */
public class JsonToCsvFn extends Expr {

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par13 {
    public Descriptor() {
      super("jsonToCsv", JsonToCsvFn.class);
    }
  }

  public JsonToCsvFn(Expr[] exprs) {
    super(exprs);
  }

  JsonToCsvFn() {}

  private static final String COMMA = ",";

  private BasicTextFullSerializer ser = new BasicTextFullSerializer();

  @Override
  public JsonValue eval(Context context) throws Exception {
    JsonValue v = exprs[0].eval(context);
    JsonArray fieldsArr = (JsonArray) exprs[1].eval(context);
    JsonString js = (JsonString) exprs[2].eval(context);
    String csv = toCsv(v, fieldsArr, js);
    return new UnquotedJsonString(csv);
  }

  /**
   * Converts the JSON value to CSV string.
   * 
   * @param jv JSON value
   * @param fieldsArr CSV field names
   * @param jDel CSV delimiter
   * @return CSV string
   * @throws Exception
   */
  String toCsv(JsonValue jv, JsonArray fieldsArr, JsonString jDel) throws Exception {
    if (jv instanceof JsonRecord) {
      return record((JsonRecord) jv, jDel);
    } else if (jv instanceof JsonArray) {
      return array((JsonArray) jv, fieldsArr, jDel);
    } else {
      return atom((JsonAtom) jv);
    }
  }

  /**
   * Returns the CSV string. For JSON array which contains only JSON arrays and
   * JSON records, {@link #table(JsonArray, JsonArray, JsonString)} is used.
   * Otherwise, {@link #iterator(JsonIterator, JsonString)} is used.
   * 
   * @param arr JSON array
   * @param fieldsArr JSON array of CSV field names
   * @param jDel CSV delimiter
   * @return CSV string
   * @throws Exception
   */
  String array(JsonArray arr, JsonArray fieldsArr, JsonString jDel) throws Exception {
    boolean isTable = true;
    for (JsonValue row : arr) {
      if (!(row instanceof JsonArray) && !(row instanceof JsonRecord)) {
        isTable = false;
        break;
      }
    }
    if (isTable) {
      return table(arr, fieldsArr, jDel);
    } else {
      return iterator(arr.iter(), jDel);
    }
  }

  /**
   * Converts the table-like JSON array. A JSON array is table-like if all of
   * its elements are JSON array or JSON record.
   * 
   * 
   * @param arr Table-like JSON array
   * @param fieldsArr JSON array of CSV field names
   * @param jDel CSV delimiter
   * @return CSV string
   * @throws IllegalArgumentException If the JSON array is not table-like.
   * @throws Exception
   */
  String table(JsonArray arr, JsonArray fieldsArr, JsonString jDel) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf);

    // field names
    JsonString[] fields = new JsonString[0];
    if (fieldsArr != null) {
      int count = (int) fieldsArr.count();
      fields = new JsonString[count];
      for (int i = 0; i < count; i++) {
        JsonString str = (JsonString) fieldsArr.get(i);
        fields[i] = str;
      }
    }
    String lineSep = "";
    for (JsonValue row : arr) {
      out.print(lineSep);
      JsonArray rowArr;
      if (row instanceof JsonRecord) {
        JsonRecord rowRec = (JsonRecord) row;
        BufferedJsonArray bufArr = new BufferedJsonArray();
        for (JsonString k : fields) {
          bufArr.add(rowRec.get(k));
        }
        rowArr = bufArr;
      } else if (row instanceof JsonArray) {
        rowArr = (JsonArray) row;
      } else {
        throw new IllegalArgumentException("JSON array '" + arr
            + "' is not table-like");
      }
      out.print(iterator(rowArr.iter(), jDel));
      lineSep = "\n";
    }
    return buf.toString();
  }

  /**
   * Converts the JSON atom to CSV string. The JSON atom is simply serialized.
   * 
   * @param v JSON atom
   * @return
   * @throws Exception
   */
  String atom(JsonAtom v) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf);
    ser.write(out, v);
    return buf.toString();
  }

  /**
   * Converts the JSON atom to CSV string. The values of JOSN record's pairs are
   * serialized.
   * 
   * @param r JSON record
   * @param jDel CSV Delimiter
   * @return CSV string
   * @throws Exception
   * @see #iterator(JsonIterator, JsonString)
   */
  String record(JsonRecord r, JsonString jDel) throws Exception {
    JsonIterator ji = JsonRecord.valueIter(r.iteratorSorted());
    return iterator(ji, jDel);
  }

  /**
   * Converts the JSON iterator to CSV string. The values from the iterator is
   * joined together using the given delimiter.
   * 
   * @param ji JSON iterator
   * @param jDel CSV Delimiter
   * @return CSV string
   * @throws Exception
   */
  private String iterator(JsonIterator ji, JsonString jDel) throws Exception {
    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf);
    String delimiter = jDel == null ? COMMA : jDel.toString();
    String sep = "";
    while (ji.moveNext()) {
      JsonValue v = ji.current();
      out.print(sep);
      ser.write(out, v);
      sep = delimiter;
    }
    return buf.toString();
  }
}
