/*
 * Copyright (C) IBM Corp. 2008.
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
package com.ibm.jaql.io.converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SubJsonString;
import com.ibm.jaql.lang.expr.del.DelOptionParser;
import com.ibm.jaql.lang.expr.string.DelParser;
import com.ibm.jaql.lang.expr.string.StringConverter;

/**
 * Base class for converters that convert a delimited file into JSON.
 * <p>
 * 
 * Incoming lines are first tokenized using <code>delimiter</code> and then,
 * optionally, converted into a JSON type. The output depends on whether field
 * names have been provided to the converter. If so, the converter produces a
 * record for each input line; otherwise, it produces an array.
 * <p>
 * 
 * The converter handle quoted values in the input data if option
 * <code>quoted</code> is set to <code>true</code>. It can be parameterized by
 * an ASCII quote character (defaults to <code>'"'</code>). Quotes may be
 * escaped using double-quoting, i.e. <code>"te""st"</code> will produce a
 * single string <code>"te\"st"</code>.
 * <p>
 * 
 * If <code>quoted</code> and <code>escape</code> are <code>true</code>,
 * 2-character escape sequences such as <code>\n</code> and 6-character escape
 * sequences such as <code>&#92;u008a</code> are unescaped. Illegal escape
 * sequences such as <code>\x</code> and <code>&#92;uwe12</code> are just
 * converted literally. For example, <code>\x</code> are just converted into
 * <code>\x</code> (2 characters) and <code>&#92;uwe12</code> are just converted
 * into <code>&#92;uwe12</code> (6 characters).
 * <p>
 * 
 * This converter is UTF-8 compatible. (This is due to the fact that ASCII
 * characters cannot occur within a multi-byte UTF-8 codepoint).
 * 
 * @see DelOptionParser
 */
public abstract class AbstractFromDelConverter<K,V> implements KeyValueImport<K, V> {
  // TODO: feature for skipping header lines
  
  public AbstractFromDelConverter()
  {
    init(null);
  }
  
  // -- constants ---------------------------------------------------------------------------------
  
  private JsonValue emptyTarget;
  private Schema schema;

  private byte delimiter;
  private DelParser reader;
  private boolean isRecord;
  private static JsonString fieldNames[];
  private static int fieldIndexes[];
  private boolean firstRow = true;
  private int noFields;
  private StringConverter converter;
  private Map<Integer,JsonValue> conversionTargets;
  
  /** Initializes this converter. */
  @Override
  public void init(JsonRecord options)
  {
    DelOptionParser handler = new DelOptionParser();
    handler.handle(options);
    
    delimiter = handler.getDelimiter();
    schema = handler.getSchema();
    
    // make reader
    reader = DelParser.make(delimiter, handler.getQuoted(), handler.getEscape());

    // check for schema
    isRecord = false;
    fieldNames = null;
    fieldIndexes = null;
    firstRow = true;
    noFields = -1;

    if (schema instanceof RecordSchema) {
      try {
        RecordSchema recordSchema = (RecordSchema)schema;
        if (recordSchema.hasAdditional() || recordSchema.noOptional()>0) {
          throw new IllegalArgumentException("record schema must not have optional or wildcard fields");
        }
        isRecord = true;
        
        // extract the field names
        List<Field> fields = recordSchema.getFieldsByPosition();
        fieldNames = new JsonString[fields.size()];
        BufferedJsonRecord target = new BufferedJsonRecord();
        for (int i=0; i<fields.size(); i++) {
          JsonString fieldName = fields.get(i).getName();
          fieldNames[i] = fieldName;
          target.add(fieldName, new SubJsonString());
        }

        // compute the indexes
        target.sort();
        fieldIndexes = new int[fields.size()];
        for (int i=0; i<fieldNames.length; i++)
        {
          fieldIndexes[i] = target.indexOf(fieldNames[i]);
          assert fieldIndexes[i]>=0;
        }
        
        // set the target
        emptyTarget = target;
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
    else if (schema instanceof ArraySchema)
    {
      ArraySchema arraySchema = (ArraySchema)schema;
      if (arraySchema.hasRest()) {
        throw new IllegalArgumentException("array schema must not have variable length");
      }
      emptyTarget = new BufferedJsonArray(arraySchema.getHeadSchemata().size());
    }
    else if (schema != null)
    {
      throw new IllegalArgumentException("only array or record schemata are accepted");
    }
    else
    {
      emptyTarget = new BufferedJsonArray(); 
    }
    
    // check for convert
    converter = null;
    conversionTargets = null;
    if (schema != null)
    {
      converter = new StringConverter(schema);
      conversionTargets = new HashMap<Integer, JsonValue>();
    }
  }

  /** Creates a fresh target. */
  @Override
  public JsonValue createTarget()
  {
    try 
    {
      return emptyTarget.getCopy(null);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /** Initialize the conversion process by looking at the first line. This methods is expected
   * to initialize the <code>noFields</code> variable. */
  private final void init(JsonValue targetValue, byte[] firstLine, int length)
  {
    if (schema == null)
    {
      // count the number of columns
      noFields = 0;
      int start = 0;
      while (start < length)
      {
        start = reader.readField(null, firstLine, length, start);
        noFields++;
      }
      
      // initialize the array
      BufferedJsonArray target = (BufferedJsonArray)targetValue;
      assert target.size() == 0;
      for (int pos = 0; pos < noFields; ++pos)
      {
        target.add(new SubJsonString());
      }
      
      // set number of fields
    }
    else
    {
      noFields = (int)schema.minElements().get();
    }
  }

  /** Clears the respective field in the target value. */
  private final void clear(JsonValue targetValue, int field)
  {
    if (isRecord)
    {
      BufferedJsonRecord target = (BufferedJsonRecord)targetValue;
      target.set(fieldIndexes[field], null);
    }
    else
    {
      BufferedJsonArray target = (BufferedJsonArray)targetValue;
      target.set(field, null);
    }
  }
  
  /** Gets the string value associated with the given field. Changes to this string will 
   * directly affect the target value. */
  private final SubJsonString get(JsonValue targetValue, int field)
  {
    SubJsonString string; 
    if (isRecord)
    {
      BufferedJsonRecord target = (BufferedJsonRecord)targetValue;
      string = (SubJsonString)target.get(fieldIndexes[field]);
      if (string == null)
      {
        string = new SubJsonString();
        target.set(fieldIndexes[field], string);
      }
    }
    else
    {
      BufferedJsonArray target = (BufferedJsonArray)targetValue;
      string = (SubJsonString)target.get(field);
      if (string == null)
      {
        string = new SubJsonString();
        target.set(field, string);
      }
    }
    
    return string;
  }
  
  /** Converts the given line into a JSON value. */
  protected final JsonValue convert(long position, byte[] bytes, int length, JsonValue target)
  {
    assert length > 0;
    
    // retrieve our own target for the given converted value
    JsonValue conversionTarget = target;
    if (converter != null)
    {
      int h = System.identityHashCode(target);
      JsonValue myTarget = conversionTargets.get(h);
      if (myTarget == null) // it's a new one
      { 
        conversionTarget = converter.createTarget();
        myTarget = target;
      }
      target = myTarget;
      h = System.identityHashCode(conversionTarget);
      conversionTargets.put(h, target);
    }
    
    // initialize 
    if (firstRow)
    {
      init(target, bytes, length);
      firstRow = false;
    }

    // go
    int start = 0;
    int field = 0;
    while (start < length && field < noFields) {
      // read the next field
      SubJsonString string = get(target, field);
      int end = reader.readField(string, bytes, length, start);
      
      // check for empty field
      if (end == start+1)
      {
        clear(target, field);
      }
      
      // advance
      start=end;
      ++field;
    }

    // special case: last field is empty
    if (length > 0 && bytes[length-1] == delimiter)
    {
      clear(target, field);
      ++ field;
    }
    
    // check that we got the right number of fields
    if (field != noFields || start < length) 
    {
      throw new RuntimeException("Wrong number of fields on input at position " + position);
    }

    // done
    if (converter == null)
    {
      return target;
    }
    else
    {
      return converter.convert(target, conversionTarget);
    }
  }
  
  @Override
  public Schema getSchema()
  {
    if (schema == null) {
      return SchemaFactory.arraySchema();
    }
    else
    {
      return schema;
    }
  }
}
