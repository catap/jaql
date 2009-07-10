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

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;
import com.ibm.jaql.util.Bool3;

/** Superclass for schemata of JSON values. Commonly used schemata can be created using the
 * {@link SchemaFactory} class. Schemata can be modified or combined using the 
 * {@link SchemaTransformation} class. */
public abstract class Schema
{
  public enum SchemaType
  {
    ANY_NON_NULL, ARRAY, BINARY, BOOLEAN, DATE, DECFLOAT, DOUBLE, GENERIC, LONG, NULL, NUMERIC, 
    OR, RECORD, STRING
  }
  
  // -- common argument names ---------------------------------------------------------------------
  
  public static final JsonString PAR_MIN = new JsonString("min");
  public static final JsonString PAR_MAX = new JsonString("max");
  public static final JsonString PAR_MIN_LENGTH = new JsonString("minLength");
  public static final JsonString PAR_MAX_LENGTH = new JsonString("maxLength");
  public static final JsonString PAR_VALUE = new JsonString("value");


  // -- schema description ------------------------------------------------------------------------

  public abstract SchemaType getSchemaType();

  /** Checks whether this schema represents a null value.
   *  
   * Returns <code>Bool3.TRUE</code> if the schema matches null values only. 
   * Returns <code>Bool3.UNKNOWN</code> if the schema matches null values and other values.
   * Returns <code>Bool3.FALSE</code> if the schema does not match null values.
   */
  public abstract Bool3 isNull();

  /** Checks whether this schema represents a constant value. 
   *  
   * Returns <code>true</code> if the schema matches only a single value.  
   * Returns <code>false</code> if the schema matches more than one value.
   */
  public abstract boolean isConstant();

  /** Checks whether this schema represents an array value.
   *  
   * Returns <code>Bool3.TRUE</code> if the schema matches array values only. 
   * Returns <code>Bool3.UNKNOWN</code> if the schema matches array values and other values.
   * Returns <code>Bool3.FALSE</code> if the schema does not match array values.
   */
  public Bool3 isArray()
  {
    return isArrayOrNull().and(isNull().not());
  }

  /** Checks whether this schema represents an array value and/or null.
   *  
   * Returns <code>Bool3.TRUE</code> if the schema matches null and/or array values only. 
   * Returns <code>Bool3.UNKNOWN</code> if the schema matches null and/or array values, and other values.
   * Returns <code>Bool3.FALSE</code> if the schema matches neither array nor null values.
   */
  public abstract Bool3 isArrayOrNull();

  /** Checks whether this schema represents an empty array.
   *  
   * Returns <code>Bool3.TRUE</code> if the schema matches empty arrays only. 
   * Returns <code>Bool3.UNKNOWN</code> if the schema matches empty arrays and other values.
   * Returns <code>Bool3.FALSE</code> if the schema does not match empty arrays.
   */
  public Bool3 isEmptyArray() 
  {
    return isEmptyArrayOrNull().and(isNull().not());
  }
  
  /** Checks whether this schema represents an empty array and/or null.
   *  
   * Returns <code>Bool3.TRUE</code> if the schema matches null and/or empty arrays only. 
   * Returns <code>Bool3.UNKNOWN</code> if the schema matches null and/or empty arrays, and other values.
   * Returns <code>Bool3.FALSE</code> if the schema matches neither empty arrays nor null values.
   */
  public abstract Bool3 isEmptyArrayOrNull();
  
  
  // -- schema matching ---------------------------------------------------------------------------
  
  /** Checks whether this schema matches the specified value or throws an exception if an
   * error occurs */
  public abstract boolean matches(JsonValue value) throws Exception;
  
  /** Checks whether this schema matches the specified value or returns false if an error 
   * occurs. */
  public boolean matchesUnsafe(JsonValue value) 
  {
    try {
      return matches(value);
    }
    catch (Exception e)
    {
     return false;    
    }
  }
  
  
  // -- schema combination ------------------------------------------------------------------------
  
  /** Merges this schema with the given schema or return <code>null</code> if such a merge is
   * not possible / desired / implemented. Most schemes will only accept arguments of their own
   * type.  
   */
  protected abstract Schema merge(Schema other);

  
  
  // -- introspection -----------------------------------------------------------------------------

  /** Returns the schema of the elements of this schema or null if this schema does not have
   * elements. Examples: (1) common schema of all elements of an array, (2) common schema
   * of all values in a record. */
  public Schema elements()
  {
    return null;
  }

  /** Checks whether this schema has the specified element. Examples: (1) index of array, 
   * (2) field name of record */ 
  public Bool3 hasElement(JsonValue which)
  {
    return Bool3.FALSE; // unsafe default!
  }
  
  /** Returns the schema of the specified element of this schema or null if this schema does not 
   * have elements. Examples: (1) schema for index of array, (2) schema for field of record */
  public Schema element(JsonValue which)
  {
    return null;
  }

  /** Returns the minimum number of elements of this schema or <code>null</code> if no minimum
   * can be determined. */
  public JsonLong minElements()
  {
    return null;
  }

  /** Returns the maximum number of elements of this schema or <code>null</code> if no maximum
   * can be determined. */
  public JsonLong maxElements()
  {
    return null; 
  }

  
  // -- printing and parsing ----------------------------------------------------------------------
  
  public String toString()
  {
    JsonSchema s = new JsonSchema(this);
    try
    {
      return JsonUtil.printToString(s);
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
}
