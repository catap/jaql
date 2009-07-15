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
package com.ibm.jaql.json.schema;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import antlr.RecognitionException;
import antlr.TokenStreamException;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/** Constructs schemata for commonly used situations */
public class SchemaFactory
{
  // -- cache -------------------------------------------------------------------------------------

  private static AnyNonNullSchema anyNonNullSchema;
  private static ArraySchema arraySchema;
  private static ArraySchema emptyArraySchema;
  private static BinarySchema binarySchema;
  private static BooleanSchema booleanSchema;
  private static DateSchema dateSchema;
  private static DecimalSchema decimalSchema;
  private static DoubleSchema doubleSchema;
  private static GenericSchema functionSchema;
  private static LongSchema longSchema;
  private static NullSchema nullSchema;
  private static RecordSchema recordSchema;
  private static RecordSchema emptyRecordSchema;
  private static StringSchema stringSchema;

  private static Schema anyOrNullSchema;
  private static Schema arrayOrNullSchema;
  private static Schema emptyArrayOrNullSchema;
  private static Schema binaryOrNullSchema;
  private static Schema booleanOrNullSchema;
  private static Schema dateOrNullSchema;
  private static Schema decimalOrNullSchema;
  private static Schema doubleOrNullSchema;
  private static Schema functionOrNullSchema;
  private static Schema longOrNullSchema;
  private static Schema recordOrNullSchema;
  private static Schema stringOrNullSchema;


  // -- commonly used schemata --------------------------------------------------------------------

  public static Schema anyNonNullSchema()
  {
    if (anyNonNullSchema  == null) anyNonNullSchema = new AnyNonNullSchema();
    return anyNonNullSchema;
  }

  public static Schema arraySchema()
  {
    if (arraySchema  == null) arraySchema  = new ArraySchema();
    return arraySchema;
  }
  
  public static Schema emptyArraySchema()
  {
    if (emptyArraySchema  == null) emptyArraySchema  = new ArraySchema(new Schema[0]);
    return emptyArraySchema;
  }


  public static Schema binarySchema()
  {
    if (binarySchema  == null) binarySchema  = new BinarySchema();
    return binarySchema;
  }
  
  public static Schema booleanSchema()
  {
    if (booleanSchema  == null) booleanSchema  = new BooleanSchema();
    return booleanSchema;
  }

  public static Schema dateSchema()
  {
    if (dateSchema  == null) dateSchema  = new DateSchema();
    return dateSchema;
  }

  public static Schema decimalSchema()
  {
    if (decimalSchema  == null) decimalSchema = new DecimalSchema();
    return decimalSchema;
  }

  public static Schema doubleSchema()
  {
    if (doubleSchema  == null) doubleSchema = new DoubleSchema();
    return doubleSchema;
  }

  public static Schema functionSchema()
  {
    if (functionSchema  == null) functionSchema = new GenericSchema(JsonType.FUNCTION);
    return functionSchema;
  }

  public static Schema longSchema()
  {
    if (longSchema  == null) longSchema = new LongSchema();
    return longSchema;
  }
  
  public static Schema nullSchema()
  {
    if (nullSchema  == null) nullSchema = new NullSchema();
    return nullSchema;
  }

  public static Schema recordSchema()
  {
    if (recordSchema  == null) recordSchema = new RecordSchema();
    return recordSchema;
  }
  
  public static Schema emptyRecordSchema()
  {
    if (emptyRecordSchema  == null) emptyRecordSchema  = new RecordSchema(new RecordSchema.Field[0], null);
    return emptyRecordSchema;
  }

  public static Schema stringSchema()
  {
    if (stringSchema  == null) stringSchema = new StringSchema();
    return stringSchema;
  }

  public static Schema anyOrNullSchema()
  {
    if (anyOrNullSchema  == null) anyOrNullSchema = SchemaTransformation.or(anyNonNullSchema(), nullSchema());
    return anyOrNullSchema;
  }

  public static Schema arrayOrNullSchema()
  {
    if (arrayOrNullSchema  == null) arrayOrNullSchema = SchemaTransformation.or(arraySchema(), nullSchema());
    return arrayOrNullSchema;
  }

  public static Schema emptyArrayOrNullSchema()
  {
    if (emptyArrayOrNullSchema  == null) emptyArrayOrNullSchema = SchemaTransformation.or(emptyArraySchema(), nullSchema());
    return emptyArrayOrNullSchema;
  }
  
  public static Schema binaryOrNullSchema()
  {
    if (binaryOrNullSchema  == null) binaryOrNullSchema = SchemaTransformation.or(binarySchema(), nullSchema());
    return binaryOrNullSchema;
  }
  
  public static Schema booleanOrNullSchema()
  {
    if (booleanOrNullSchema  == null) booleanOrNullSchema = SchemaTransformation.or(booleanSchema(), nullSchema());
    return booleanOrNullSchema;
  }

  public static Schema dateOrNullSchema()
  {
    if (dateOrNullSchema  == null) dateOrNullSchema = SchemaTransformation.or(dateSchema(), nullSchema());
    return dateOrNullSchema;
  }

  public static Schema decimalOrNullSchema()
  {
    if (decimalOrNullSchema  == null) decimalOrNullSchema = SchemaTransformation.or(decimalSchema(), nullSchema());
    return decimalOrNullSchema;
  }

  public static Schema doubleOrNullSchema()
  {
    if (doubleOrNullSchema  == null) doubleOrNullSchema = SchemaTransformation.or(doubleSchema(), nullSchema());
    return doubleOrNullSchema;
  }
  
  public static Schema functionOrNullSchema()
  {
    if (functionOrNullSchema  == null) functionOrNullSchema = SchemaTransformation.or(functionSchema(), nullSchema());
    return functionOrNullSchema;
  }
  public static Schema longOrNullSchema()
  {
    if (longOrNullSchema  == null) longOrNullSchema = SchemaTransformation.or(longSchema(), nullSchema());
    return longOrNullSchema;
  }

  public static Schema recordOrNullSchema()
  {
    if (recordOrNullSchema  == null) recordOrNullSchema = SchemaTransformation.or(recordSchema(), nullSchema());
    return recordOrNullSchema;
  }
  
  public static Schema stringOrNullSchema()
  {
    if (stringOrNullSchema  == null) stringOrNullSchema = SchemaTransformation.or(stringSchema(), nullSchema());
    return stringOrNullSchema;
  }
  
  
  // -- construction for the parser ---------------------------------------------------------------
  
  /**
   * @param typeConstructorName
   * @return The paramater descriptor for the constructor function with the specified type name.
   */
  public static Parameters getParameters(String typeConstructorName)
  {
    // TODO: this code should be table-driven. It requires Parameter factory classes.
    if( typeConstructorName.equals("any") )
    {
      return new Parameters();
    } 
    else if( typeConstructorName.equals("binary") )
    {
      return BinarySchema.getParameters();
    }
    else if( typeConstructorName.equals("boolean") )
    {
      return BooleanSchema.getParameters();
    }
    else if( typeConstructorName.equals("date") )
    {
      return DateSchema.getParameters();
    }
    else if( typeConstructorName.equals("decfloat") )
    {
      return DecimalSchema.getParameters();
    }
    else if( typeConstructorName.equals("double") )
    {
      return DoubleSchema.getParameters();
    }
    else if( typeConstructorName.equals("function") )
    {
      return GenericSchema.getParameters();
    }
    else if( typeConstructorName.equals("long") )
    {
      return LongSchema.getParameters();
    }
    else if( typeConstructorName.equals("schema") )
    {
      return GenericSchema.getParameters();
    }
    else if( typeConstructorName.equals("string") )
    {
      return StringSchema.getParameters();
    }
    else 
    throw new RuntimeException("undefined type: " + typeConstructorName);
  }

  public static Schema make(String typeConstructorName, JsonRecord args)
  {
    // TODO: this code should be table-driven.
    if( typeConstructorName.equals("any") )
    {
      return SchemaFactory.anyNonNullSchema();
    }
    else if( typeConstructorName.equals("binary") )
    {
      return new BinarySchema(args);
    }
    else if( typeConstructorName.equals("boolean") )
    {
      return new BooleanSchema(args);
    }
    else if( typeConstructorName.equals("date") )
    {
      return new DateSchema(args);
    }
    else if( typeConstructorName.equals("decfloat") )
    {
      return new DecimalSchema(args);
    }
    else if( typeConstructorName.equals("double") )
    {
      return new DoubleSchema(args);
    }
    else if( typeConstructorName.equals("function") )
    {
      return new GenericSchema(JsonType.FUNCTION, args);
    }
    else if( typeConstructorName.equals("long") )
    {
      return new LongSchema(args);
    }
    else if( typeConstructorName.equals("schema") )
    {
      return new GenericSchema(JsonType.SCHEMA, args);
    }
    else if( typeConstructorName.equals("string") )
    {
      return new StringSchema(args);
    }
    throw new RuntimeException("undefined type: "+typeConstructorName);
  }
  
  
  // -- schema inference --------------------------------------------------------------------------
  
  /** Tries to construct the tightest possible schema that matches the given value */
  public static Schema schemaOf(JsonValue v)
  {
    if (v == null)
    {
      return nullSchema();
    }
    
    JsonLong length;
    switch (v.getEncoding().getType())
    {
    case ARRAY: 
      List<Schema> schemata = new ArrayList<Schema>();
      for (JsonValue vv : (JsonArray)v)
      {
        schemata.add(schemaOf(vv));
      }
      return new ArraySchema(schemata.toArray(new Schema[schemata.size()]));
      
    case RECORD:
      List<RecordSchema.Field> fields = new ArrayList<RecordSchema.Field>();
      for (Entry<JsonString, JsonValue> e : (JsonRecord)v)
      {
        JsonString name = e.getKey();
        JsonValue value = e.getValue();
        fields.add(new RecordSchema.Field(name, schemaOf(value), true));
      }
      return new RecordSchema(fields.toArray(new RecordSchema.Field[fields.size()]), null);
      
    case BOOLEAN:
      return new BooleanSchema((JsonBool)v);
      
    case STRING:
      JsonString js = (JsonString)v;
      return new StringSchema(null, null, null, js);
      
    case NUMBER:
      switch (v.getEncoding())
      {
      case DECIMAL:
        return new DecimalSchema(null, null, (JsonDecimal)v);
      case LONG:
        return new LongSchema(null, null, (JsonLong)v);
      default:
        throw new IllegalStateException(); // only reached when new number encodings are added
      }

    case DOUBLE:
      return new DoubleSchema(null, null, (JsonDouble)v);
      
    // JSON extensions

    case BINARY:
      JsonBinary jb = (JsonBinary)v;
      length = new JsonLong(jb.length());
      return new BinarySchema(length, length);
      
    case DATE:
      JsonDate jd = (JsonDate)v;
      return new DateSchema(null, null, jd);
      
    case SCHEMA:
      return new GenericSchema(JsonType.SCHEMA);
    
    case FUNCTION:
      return new GenericSchema(JsonType.FUNCTION);
      
    case JAVAOBJECT:
    case REGEX:
    default:
      return anyNonNullSchema();
    }
  }

  /** Parse a schema from the specified string. The string must not contain the "schema" keyword
   * of Jaql, e.g., <code>long</code> is valid but <code>schema long</code> is not. */
  public static final Schema parse(String s) throws IOException
  {
    
    JaqlLexer lexer = new JaqlLexer(new StringReader(s));
    JaqlParser parser = new JaqlParser(lexer);
    try
    {
      return parser.schema();
    } catch (RecognitionException e)
    {
      throw new IOException(e);
    } catch (TokenStreamException e)
    {
      throw new IOException(e);
    }
  }
}
