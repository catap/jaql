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
import java.util.Map;

import com.ibm.jaql.io.hadoop.converter.KeyValueImport;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SubJsonString;
import com.ibm.jaql.lang.expr.string.StringConverter;
import com.ibm.jaql.lang.expr.string.DelParser;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Base class for converters that convert a delimited file into JSON. The default implementation
 * can be found in the vendor package.
 * 
 * Incoming lines are first tokenized using an ASCII delimiter and then, optionally,
 * converted into a JSON type. The output depends on whether field names have been provided to 
 * the converter. If so, the converter produces a record for each input line; otherwise, it 
 * produces an array. 
 * 
 * The converter has support for quoted values in the input data. It can be parametrized
 * by an ASCII quote character (defaults to <code>'"'</code>). Quotes may be escaped using 
 * double-quoting, i.e. <code>"te""st"</code> will produce a single string <code>"te\"st"</code>.
 * 
 * This converter is UTF-8 compatible. (This is due to the fact that ASCII characters
 * cannot occur within a multi-byte UTF-8 codepoint).
 */
public abstract class AbstractFromDelConverter<K,V> implements KeyValueImport<K, V> {
  
  // -- constants ---------------------------------------------------------------------------------
  
  public static final JsonString DELIMITER_NAME = new JsonString("delimiter");
  public static final byte DELIMITER_DEFAULT = ',';
  public static final JsonString QUOTED_NAME = new JsonString("quoted");
  public static final boolean QUOTED_DEFAULT = true;
  public static final JsonString FIELDS_NAME = new JsonString("fields");
  public static final JsonString CONVERT_NAME = new JsonString("convert");

  // TODO: feature for skipping header lines
  

  public AbstractFromDelConverter()
  {
    init(null);
  }
  
  // -- constants ---------------------------------------------------------------------------------
  
  private JsonValue emptyTarget;
  private Schema schema;

  private byte delimiter;
  boolean quoted;
  private DelParser reader;
  private JsonArray header;
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
    // TODO: better error reporting when handling arguments
    if (options == null) options = JsonRecord.EMPTY;
    
    JsonValue arg;
    
    // check for delimiter/quote override
    delimiter = DELIMITER_DEFAULT;
    if (options.containsKey(DELIMITER_NAME))
    {
      delimiter = getCharacter(options, DELIMITER_NAME, false);
    }
    
    quoted = QUOTED_DEFAULT;
    if (options.containsKey(QUOTED_NAME))
    {
      JsonValue value = options.get(QUOTED_NAME);
      if (value == null)
      {
        throw new IllegalArgumentException("parameter \"" + QUOTED_NAME + "\" must not be null");
      }
      if (!(value instanceof JsonBool))
      {
        throw new IllegalArgumentException("parameter \"" + QUOTED_NAME + "\" must be boolean");
      }
      quoted = ((JsonBool)value).get();
    }
    
    // make reader
    reader = DelParser.make(delimiter, quoted);

    // check for header
    fieldNames = null;
    fieldIndexes = null;
    firstRow = true;
    noFields = -1;
    arg = options.get(FIELDS_NAME);
    if (arg != null) {
      try {
        // extract the field names
        header = (JsonArray) arg;
        fieldNames = new JsonString[(int)header.count()];
        BufferedJsonRecord target = new BufferedJsonRecord(); 
        int i=0;
        for(JsonValue jv: header.iter()){
          fieldNames[i] = JaqlUtil.enforceNonNull(((JsonString)jv).getImmutableCopy());
          target.add(fieldNames[i], new SubJsonString());
          i++;
        }

        // compute the indexes
        target.sort();
        fieldIndexes = new int[fieldNames.length];
        for (i=0; i<fieldNames.length; i++)
        {
          fieldIndexes[i] = target.indexOf(fieldNames[i]);
          assert fieldIndexes[i]>=0;
        }
        
        // set the target
        emptyTarget = target;
        schema = new RecordSchema(fieldNames);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
    else
    {
      // no header
      header = null;
      emptyTarget = new BufferedJsonArray();
      schema = SchemaFactory.arraySchema();
    }
    
    // check for convert
    converter = null;
    conversionTargets = null;
    arg = options.get(CONVERT_NAME);
    if (arg != null)
    {
      // TODO: check schema compatibility
      schema = ((JsonSchema)arg).get();
      converter = new StringConverter(schema);
      conversionTargets = new HashMap<Integer, JsonValue>();
    }
  }

  /** Checks whether <code>arg</code> consists of a single character and returns it, if so. 
   * Otherwise, fails with an exception. If <code>allowNull</code> is set, returns 
   * <code>null</code> on <code>null</code> input, otherwise fails on <code>null</code> input. */
  private final Byte getCharacter(JsonRecord record, JsonString name, boolean allowNull)
  {
    JsonValue arg = record.get(name);
    
    // check for null
    if (arg == null)
    {
      if (allowNull) return null;
      throw new IllegalArgumentException("parameter \"" + name.toString() + "\" must not be null");
    }
    
    // check for type and length
    if (!(arg instanceof JsonString))
    {
      throw new IllegalArgumentException("parameter \"" + name.toString() + "\" must be a string");
    }
    JsonString s = (JsonString)arg;
    if (s.bytesLength() != 1)
    {
      throw new RuntimeException("parameter \"" + name.toString() + "\" must be consist of a single character");
    }

    // return it
    return s.get(0);
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
    if (header == null)
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
      noFields = (int)header.count();
    }
  }

  /** Clears the respective field in the target value. */
  private final void clear(JsonValue targetValue, int field)
  {
    if (header == null)
    {
      BufferedJsonArray target = (BufferedJsonArray)targetValue;
      target.set(field, null);
    }
    else
    {
      BufferedJsonRecord target = (BufferedJsonRecord)targetValue;
      target.set(fieldIndexes[field], null);
    }
  }
  
  /** Gets the string value associated with the given field. Changes to this string will 
   * directly affect the target value. */
  private final SubJsonString get(JsonValue targetValue, int field)
  {
    SubJsonString string; 
    if (header == null)
    {
      BufferedJsonArray target = (BufferedJsonArray)targetValue;
      string = (SubJsonString)target.get(field);
      if (string == null)
      {
        string = new SubJsonString();
        target.set(field, string);
      }
    }
    else
    {
      BufferedJsonRecord target = (BufferedJsonRecord)targetValue;
      string = (SubJsonString)target.get(fieldIndexes[field]);
      if (string == null)
      {
        string = new SubJsonString();
        target.set(fieldIndexes[field], string);
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
    return schema;
  }
}