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
import java.lang.reflect.UndeclaredThrowableException;

import antlr.RecognitionException;
import antlr.TokenStreamException;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.util.Bool3;

/** Superclass for schemata of JSON values. */
public abstract class Schema
{
  public enum SchemaType
  {
    ANY, ARRAY, BINARY, BOOLEAN, DATE, DECFLOAT, DOUBLE, GENERIC, LONG, NULL, NUMERIC, OR, RECORD, STRING
  }
  
  // -- common argument names ---------------------------------------------------------------------
  
  public static final JsonString PAR_MIN = new JsonString("min");
  public static final JsonString PAR_MAX = new JsonString("max");
  public static final JsonString PAR_MIN_LENGTH = new JsonString("minLength");
  public static final JsonString PAR_MAX_LENGTH = new JsonString("maxLength");
  public static final JsonString PAR_VALUE = new JsonString("value");

  // -- abstract methods --------------------------------------------------------------------------

  public abstract SchemaType getSchemaType();

  public abstract Bool3 isNull();
  
  public abstract Bool3 isConst();
  
  public abstract Bool3 isArray();
  
  public abstract boolean matches(JsonValue value) throws Exception;
  
  
  // -- printing and parsing ----------------------------------------------------------------------
  
  
  public String toString()
  {
    JsonSchema s = new JsonSchema(this);
    try
    {
      return JsonValue.printToString(s);
    } catch (IOException e)
    {
      throw new UndeclaredThrowableException(e);
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
  
  /**
   * @param typeConstructorName
   * @return The paramater descriptor for the constructor function with the specified type name.
   */
  public static Parameters getParameters(String typeConstructorName)
  {
    // TODO: this code should be table-driven. It requires Parameter factory classes.
    if( typeConstructorName.equals("string") )
    {
      return StringSchema.getParameters();
    }
    else if( typeConstructorName.equals("long") )
    {
      return LongSchema.getParameters();
    }
    else if( typeConstructorName.equals("double") )
    {
      return DoubleSchema.getParameters();
    }
    else if( typeConstructorName.equals("decfloat") )
    {
      return DecimalSchema.getParameters();
    }
    else if( typeConstructorName.equals("date") )
    {
      return DateSchema.getParameters();
    }
    else if( typeConstructorName.equals("boolean") )
    {
      return BooleanSchema.getParameters();
    }
    else if( typeConstructorName.equals("binary") )
    {
      return BinarySchema.getParameters();
    }
    else if( typeConstructorName.equals("function") )
    {
      return GenericSchema.getParameters();
    }
    else if( typeConstructorName.equals("schema") )
    {
      return GenericSchema.getParameters();
    }
    throw new RuntimeException("undefined type: "+typeConstructorName);
  }

  public static Schema make(String typeConstructorName, JsonRecord args)
  {
    // TODO: this code should be table-driven. It requires factory classes.
    if( typeConstructorName.equals("string") )
    {
      return new StringSchema(args);
    }
    else if( typeConstructorName.equals("long") )
    {
      return new LongSchema(args);
    }
    else if( typeConstructorName.equals("double") )
    {
      return new DoubleSchema(args);
    }
    else if( typeConstructorName.equals("decfloat") )
    {
      return new DecimalSchema(args);
    }
    else if( typeConstructorName.equals("date") )
    {
      return new DateSchema(args);
    }
    else if( typeConstructorName.equals("boolean") )
    {
      return new BooleanSchema(args);
    }
    else if( typeConstructorName.equals("binary") )
    {
      return new BinarySchema(args);
    }
    else if( typeConstructorName.equals("function") )
    {
      return new GenericSchema(JsonType.FUNCTION, args);
    }
    else if( typeConstructorName.equals("schema") )
    {
      return new GenericSchema(JsonType.SCHEMA, args);
    }
    throw new RuntimeException("undefined type: "+typeConstructorName);
  }
}
