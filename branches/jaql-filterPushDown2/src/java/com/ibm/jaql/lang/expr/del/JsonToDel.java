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
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.def.BoolSerializer;
import com.ibm.jaql.io.serialization.text.def.DecimalSerializer;
import com.ibm.jaql.io.serialization.text.def.DoubleSerializer;
import com.ibm.jaql.io.serialization.text.def.LongSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonEncoding;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.json.util.JsonUtil;
import com.ibm.jaql.util.RandomAccessBuffer;

/**
 * Converts a JSON value to a del(delimited) string.
 * 
 * Conversion is driven by an option record. Most of the information in the
 * <code>schema</code> field is currently ignored; there is no schema checking.
 * For records, the <code>schema</code> field determines the order of the
 * record fields in the serialized file.
 * 
 * @see DelOptionParser
 */

// TODO This is a *very* basic implementation. The converter currently should
// check whether the input conforms with the specified schema. See
// AbstractFromDelConverter.
public class JsonToDel {

  public static final String UTF_8 = "UTF-8";

  public static final byte DOUBLE_QUOTE = '"';
  public static final byte BACK_SLASH = '\\';

  private byte delimiter;
  private boolean quoted;
  private boolean escape;

  private JsonString[] fieldNames = new JsonString[0];

  private final RandomAccessBuffer buf = new RandomAccessBuffer();
  private final PrintStream out;
  
  private final Text text = new Text();

  private final RandomAccessBuffer fieldBuf = new RandomAccessBuffer();
  private final PrintStream fieldOut;

  private final List<JsonValue> values = new ArrayList<JsonValue>();

  private final TextFullSerializer fullSer = TextFullSerializer.getDefault();
  private final EnumMap<JsonEncoding, TextBasicSerializer<?>> serializers;

  public JsonToDel() {
    this(null);
  }

  /**
   * Constructs a converter with the given initialization options.
   * 
   * @param options Initialization options.
   */
  public JsonToDel(JsonRecord options) {
    DelOptionParser parser = new DelOptionParser();
    parser.handle(options);

    Schema schema = parser.getSchema();
    quoted = parser.getQuoted();
    escape = parser.getEscape();
    delimiter = parser.getDelimiter();

    if (schema instanceof RecordSchema) {
      RecordSchema recordSchema = (RecordSchema) schema;
      if (recordSchema.hasAdditional() || recordSchema.noOptional() > 0) {
        throw new IllegalArgumentException("record schema must not have optional or wildcard fields");
      }

      // extract the field names
      List<Field> fields = recordSchema.getFieldsByPosition();
      fieldNames = new JsonString[fields.size()];
      for (int i = 0; i < fields.size(); i++) {
        JsonString fieldName = fields.get(i).getName();
        fieldNames[i] = fieldName;
      }
    } else if (schema instanceof ArraySchema) {
      // silently accept
    } else if (schema != null) {
      throw new IllegalArgumentException("only array or record schemata are accepted");
    }

    try {
      out = new PrintStream(buf, false, UTF_8);
      fieldOut = new PrintStream(fieldBuf, false, UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    serializers = new EnumMap<JsonEncoding, TextBasicSerializer<?>>(JsonEncoding.class);
    serializers.put(JsonEncoding.BOOLEAN, new BoolSerializer());
    serializers.put(JsonEncoding.LONG, new LongSerializer());
    serializers.put(JsonEncoding.DECFLOAT, new DecimalSerializer());
    serializers.put(JsonEncoding.DOUBLE, new DoubleSerializer());
  }
  
  /**
   * Converts the given JSON value for a del line into a Text.
   * 
   * @param src JSON value
   * @return text text
   * @throws IOException
   * @throws IllegalArgumentException If no field names are provided for JSON
   *           record.
   */
  public void convert(JsonValue src, Text text) throws IOException {
    /*
     * 1. If quoted is false and src can be extract as a JSON string, its   
     *    internal UTF-8 byte array is set to text.
     * 2. Extract JSON values into an array list. Looping the JSON values in the
     *    array list:
     * 2.1 quotes is false: If the value is a JSON string, its internal
     *     UTF-8 byte array is written to out. Otherwise, it is serialized
     *     to out.
     * 2.2 quoted is true: If the value does not contain characters which 
     *     need double-quote escaping or backslash escaping, it is 
     *     serialized to out. For JSON string, its internal UTF-8 byte array is 
     *     written to out byte-by-byte after escaping. For the value of other
     *     types, it is serialized to a field buffer. Then the array backing the
     *     field buffer is written to out byte-by-bye after escaping.
     * 2.3 Set the content of the buffer backing out to text.
     */
    try {
      if (!quoted) {
        if (src instanceof JsonRecord && fieldNames.length == 1) {
          JsonRecord rec = (JsonRecord) src;
          JsonValue only = rec.get(fieldNames[0]);
          if (only instanceof JsonString) {
            JsonString js = (JsonString) only;
            text.set(js.getInternalBytes(), js.bytesOffset(), js.bytesLength());
            return;
          }
        } else if (src instanceof JsonArray) {
          JsonArray arr = (JsonArray) src;
          if (arr.count() == 1) {
            JsonValue only = arr.get(0);
            if (only instanceof JsonString) {
              JsonString js = (JsonString) only;
              text.set(js.getInternalBytes(), js.bytesOffset(), js.bytesLength());
              return;
            }
          }
        } else if (src instanceof JsonString) {
          JsonString js = (JsonString) src;
          text.set(js.getInternalBytes(), js.bytesOffset(), js.bytesLength());
          return;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    values.clear();
    if (src instanceof JsonRecord) {
      JsonRecord rec = (JsonRecord) src;
      if (fieldNames.length < 1)
        throw new IllegalArgumentException("fields are required to convert A JSON record into a del line.");
      for (JsonString n : fieldNames)
        values.add(rec.get(n));
    } else if (src instanceof JsonArray) {
      JsonArray arr = (JsonArray) src;
      for (JsonValue value : arr)
        values.add(value);
    } else {
      // If the value is not JsonRecord or JsonArray, then write it directly.
      values.add(src);
    } 

    buf.reset();
    for (int i = 0; i < values.size(); i++) {
      JsonValue value = values.get(i);
      if (i != 0)
        out.write(delimiter);
      if (value == null)
        continue;
      if (quoted)
        printFieldQuoted(out, value, escape);
      else
        printFieldUnquoted(out, value);
    }
    out.flush();
    text.set(buf.getBuffer(), 0, buf.size());
  }
  
  @SuppressWarnings("unchecked")
  private void printFieldQuoted(PrintStream out, JsonValue value, boolean escape)
      throws IOException {
    JsonEncoding encoding = value.getEncoding();
    TextBasicSerializer serializer = serializers.get(encoding);
    if (serializer != null) {
      serializer.write(out, value);
      return;
    }

    if (value instanceof JsonString) {
      JsonString js = (JsonString) value;
      printUtf8Quoted(out, escape, js.getInternalBytes(), js.bytesOffset(), js.bytesLength());
    } else {
      fieldBuf.reset();
      fullSer.write(fieldOut, value);
      fieldOut.flush();
      printUtf8Quoted(out, escape, fieldBuf.getBuffer(), 0, fieldBuf.size());
    }
  }

  private void printUtf8Quoted(OutputStream out,
                               boolean escape,
                               byte[] utf8,
                               int offset, 
                               int length) throws IOException {
    out.write(DOUBLE_QUOTE);
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      byte b = utf8[i];
      if (b == DOUBLE_QUOTE) {
        out.write(DOUBLE_QUOTE);
        out.write(DOUBLE_QUOTE);
      } else if (escape) {
        switch (b) {
          case '\'' :
            out.write(BACK_SLASH);
            out.write('\\');
            break;
          case '\\' :
            out.write(BACK_SLASH);
            out.write(BACK_SLASH);
            break;
          case '\b' :
            out.write(BACK_SLASH);
            out.write('b');
            break;
          case '\f' :
            out.write(BACK_SLASH);
            out.write('f');
            break;
          case '\n' :
            out.write(BACK_SLASH);
            out.write('n');
            break;
          case '\r' :
            out.write(BACK_SLASH);
            out.write('r');
            break;
          case '\t' :
            out.write(BACK_SLASH);
            out.write('t');
            break;
          default :
            int currentByte = 0x000000FF & b;
            int cp = 0;
            // single-byte encoding?
            if ((currentByte & 0x00000080) == 0) {
              cp = currentByte;
              // start of 2-byte sequence?
            } else if ((currentByte & 0x000000E0) == 0x000000C0) {
              int nextByte = 0x000000FF & utf8[++i];
              // compute the code point
              cp = ((0x0000001F & currentByte) << 6) + (0x0000003F & nextByte);
            } else {
              out.write(b);
              break;
            }

            if (Character.isISOControl(cp)) {
              out.write(BACK_SLASH);
              out.write('u');
              out.write(JsonUtil.hex[((cp & 0xf000) >>> 12)]);
              out.write(JsonUtil.hex[((cp & 0x0f00) >>> 8)]);
              out.write(JsonUtil.hex[((cp & 0x00f0) >>> 4)]);
              out.write(JsonUtil.hex[(cp & 0x000f)]);
            } else {
              out.write(b);
            }
        }
      } else {
        out.write(b);
      }
    }
    out.write(DOUBLE_QUOTE);
  }

  private void printFieldUnquoted(PrintStream out, JsonValue value)
      throws IOException {
    if (value instanceof JsonString) {
      JsonString js = (JsonString) value;
      byte[] bytes = js.getInternalBytes();
      out.write(bytes);
    } else {
      fullSer.write(out, value);
    }
  }

  /**
   * Converts the JSON value into a JSON string for a del line.
   * 
   * @param src JSON value
   * @return JSON string for a del line
   * @throws IOException
   */
  public JsonString convertToJsonString(JsonValue src) throws IOException {
    convert(src, text);
    JsonString line = new JsonString(text.getBytes(), text.getLength());
    return line;
  }

  /**
   * Converts the JSON iterator into a JSON iterator for del lines.
   * 
   * @param it JSON iterator
   * @return JSON iterator for del lines
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
