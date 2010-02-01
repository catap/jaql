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
package com.ibm.jaql.lang.expr.del;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import com.ibm.jaql.io.converter.AbstractFromDelConverter;
import com.ibm.jaql.io.serialization.text.basic.BasicTextFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.RandomAccessBuffer;

/**
 * Converts a JSON value to a delimited string.  
 * 
 * Conversion is driven by an option record. Currently, only the <code>delimited</code> 
 * and <code>schema</code> fields of the option records are used. Valid arguments for the 
 * <code>schema</code> field are: <code>null</code>, an array schema, or a record schema. Most 
 * of the information in the schema field is currently ignored; there is no schema checking. 
 * For records, the schema field determines the order of the records field in the serialized file. 
 * 
 */

// TODO This is a *very* basic implementation. The converter currently should check whether the 
// TODO input conforms with the specified schema. It should also support the "quotes" field. 
// TODO See AbstractFromDelConverter.
public class JsonToDel {

  private JsonString[] fieldNames = new JsonString[0];
  private String delimiter;
  private RandomAccessBuffer buf = new RandomAccessBuffer();
  private PrintStream out = new PrintStream(buf);
  private final BasicTextFullSerializer serializer = new BasicTextFullSerializer();

  public JsonToDel() {
    this(null);
  }

  /**
   * Constructs a converter with the given initialization options.
   * 
   * @param options Initialization options.
   */
  public JsonToDel(JsonRecord options) {
    options = options == null ? JsonRecord.EMPTY : options;
    JsonString js = (JsonString) options.get(AbstractFromDelConverter.DELIMITER_NAME);
    delimiter = (js == null) ? "," : js.toString();
    
    // TODO: remove check for deprecated options
    if (options.containsKey(new JsonString("convert"))) {
      throw new IllegalArgumentException(
          "The \"convert\" option is deprecated. Use the \"schema\" option instead.");
    }
    
    JsonSchema jsonSchema = (JsonSchema) options.get(AbstractFromDelConverter.SCHEMA_NAME);
    Schema schema = jsonSchema != null ? jsonSchema.get() : null;
    if (schema instanceof RecordSchema) {
      RecordSchema recordSchema = (RecordSchema)schema;
      if (recordSchema.hasAdditional() || recordSchema.noOptional()>0) {
        throw new IllegalArgumentException("record schema must not have optional or wildcard fields");
      }

      // extract the field names
      List<Field> fields = recordSchema.getFieldsByPosition();
      fieldNames = new JsonString[fields.size()];
      for (int i=0; i<fields.size(); i++) {
        JsonString fieldName = fields.get(i).getName();
        fieldNames[i] = fieldName;
      }
    } else if (schema instanceof ArraySchema) {
      // silently accept
    }
    else if (schema != null) {
      throw new IllegalArgumentException("only array or record schemata are accepted");
    }
  }

  /**
   * Converts the given JSON value into a buffer for a CSV line.
   * 
   * @param src JSON value
   * @return The buffer contains the byte array of UTF-8 string.
   * @throws IOException
   * @throws IllegalArgumentException If the JSON value can be converted to a
   *           CSV line.
   */
  public RandomAccessBuffer convert(JsonValue src) throws IOException {
    buf.reset();
    String sep = "";
    if (src instanceof JsonRecord) {
      JsonRecord rec = (JsonRecord) src;
      if (fieldNames.length < 1)
      	throw new IllegalArgumentException("fields are required to convert A JSON record into a CSV line.");
      for (JsonString n : fieldNames) {
        out.print(sep);
        JsonValue value = rec.get(n);
        serializer.write(out, value);
        sep = delimiter;
      }
    } else if (src instanceof JsonArray) {
      JsonArray arr = (JsonArray) src;
      for (JsonValue value : arr) {
        out.print(sep);
        serializer.write(out, value);
        sep = delimiter;
      }
    } else {
      throw new IllegalArgumentException("Type cannot be placed in delimited file.  Array or record expected: "
          + src);
    }
    out.flush();
    return buf;
  }

  /**
   * Converts the JSON value into a JSON string for a CSV line.
   * 
   * @param src JSON value
   * @return JSON string for a CSV line
   * @throws IOException
   */
  public JsonString convertToJsonString(JsonValue src) throws IOException {
    RandomAccessBuffer buf = convert(src);
    JsonString line = new JsonString(buf.getBuffer(), buf.size());
    return line;
  }

  /**
   * Converts the JSON iterator into a JSON iterator for CSV lines.
   * 
   * @param it JSON iterator
   * @return JSON iterator for CSV lines
   */
  public JsonIterator convertToJsonIterator(final JsonIterator it) {
    return new JsonIterator() {
      @Override
      public boolean moveNext() throws Exception {
        boolean moveNext = it.moveNext();
        if (moveNext) {
          JsonValue v = it.current();
          currentValue = convertToJsonString(v);
          return true;
        } else {
          return false;
        }
      }
    };
  }
}
