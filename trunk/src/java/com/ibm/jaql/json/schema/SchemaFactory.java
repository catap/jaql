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
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

/** Constructs schemata for commonly used situations */
public class SchemaFactory
{
  // -- cache -------------------------------------------------------------------------------------

  private static NonNullSchema nonNullSchema;
  private static ArraySchema arraySchema;
  private static ArraySchema emptyArraySchema;
  private static BinarySchema binarySchema;
  private static BooleanSchema booleanSchema;
  private static DateSchema dateSchema;
  private static Schema schematypeSchema;
  private static DecfloatSchema decfloatSchema;
  private static DoubleSchema doubleSchema;
  private static FunctionSchema functionSchema;
  private static LongSchema longSchema;
  private static NullSchema nullSchema;
  private static RecordSchema recordSchema;
  private static RecordSchema emptyRecordSchema;
  private static StringSchema stringSchema;
  private static Schema numericSchema;
  private static Schema numberSchema;
  
  private static Schema anySchema;
  private static Schema arrayOrNullSchema;
  private static Schema emptyArrayOrNullSchema;
  private static Schema binaryOrNullSchema;
  private static Schema booleanOrNullSchema;
  private static Schema dateOrNullSchema;
  private static Schema schematypeOrNullSchema;
  private static Schema decfloatOrNullSchema;
  private static Schema doubleOrNullSchema;
  private static Schema functionOrNullSchema;
  private static Schema longOrNullSchema;
  private static Schema recordOrNullSchema;
  private static Schema stringOrNullSchema;
  private static Schema numericOrNullSchema;
  private static Schema numberOrNullSchema;

  // -- commonly used schemata --------------------------------------------------------------------

  public static Schema nonNullSchema()
  {
    if (nonNullSchema  == null) nonNullSchema = new NonNullSchema();
    return nonNullSchema;
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

  public static Schema schematypeSchema()
  {
    if (schematypeSchema  == null) schematypeSchema  = new SchematypeSchema();
    return schematypeSchema;
  }

  public static Schema decfloatSchema()
  {
    if (decfloatSchema  == null) decfloatSchema = new DecfloatSchema();
    return decfloatSchema;
  }

  public static Schema doubleSchema()
  {
    if (doubleSchema  == null) doubleSchema = new DoubleSchema();
    return doubleSchema;
  }

  public static Schema functionSchema()
  {
    if (functionSchema  == null) functionSchema = new FunctionSchema();
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

  public static Schema numericSchema()
  {
    if (numericSchema  == null) {
      numericSchema = OrSchema.make(longSchema(), doubleSchema(), decfloatSchema());
    }
    return numericSchema;
  }

  public static Schema numberSchema()
  {
    if (numberSchema  == null) {
      numberSchema = OrSchema.make(longSchema(), decfloatSchema());
    }
    return numberSchema;
  }

  public static Schema anySchema()
  {
    if (anySchema  == null) anySchema = OrSchema.make(nonNullSchema(), nullSchema());
    return anySchema;
  }

  public static Schema arrayOrNullSchema()
  {
    if (arrayOrNullSchema  == null) arrayOrNullSchema = OrSchema.make(arraySchema(), nullSchema());
    return arrayOrNullSchema;
  }

  public static Schema emptyArrayOrNullSchema()
  {
    if (emptyArrayOrNullSchema  == null) emptyArrayOrNullSchema = OrSchema.make(emptyArraySchema(), nullSchema());
    return emptyArrayOrNullSchema;
  }
  
  public static Schema binaryOrNullSchema()
  {
    if (binaryOrNullSchema  == null) binaryOrNullSchema = OrSchema.make(binarySchema(), nullSchema());
    return binaryOrNullSchema;
  }
  
  public static Schema booleanOrNullSchema()
  {
    if (booleanOrNullSchema  == null) booleanOrNullSchema = OrSchema.make(booleanSchema(), nullSchema());
    return booleanOrNullSchema;
  }

  public static Schema dateOrNullSchema()
  {
    if (dateOrNullSchema  == null) dateOrNullSchema = OrSchema.make(dateSchema(), nullSchema());
    return dateOrNullSchema;
  }
  
  public static Schema schematypeOrNullSchema()
  {
    if (schematypeOrNullSchema  == null) schematypeOrNullSchema = OrSchema.make(schematypeSchema(), nullSchema());
    return schematypeOrNullSchema;
  }

  public static Schema decfloatOrNullSchema()
  {
    if (decfloatOrNullSchema  == null) decfloatOrNullSchema = OrSchema.make(decfloatSchema(), nullSchema());
    return decfloatOrNullSchema;
  }

  public static Schema doubleOrNullSchema()
  {
    if (doubleOrNullSchema  == null) doubleOrNullSchema = OrSchema.make(doubleSchema(), nullSchema());
    return doubleOrNullSchema;
  }
  
  public static Schema functionOrNullSchema()
  {
    if (functionOrNullSchema  == null) functionOrNullSchema = OrSchema.make(functionSchema(), nullSchema());
    return functionOrNullSchema;
  }
  public static Schema longOrNullSchema()
  {
    if (longOrNullSchema  == null) longOrNullSchema = OrSchema.make(longSchema(), nullSchema());
    return longOrNullSchema;
  }

  public static Schema recordOrNullSchema()
  {
    if (recordOrNullSchema  == null) recordOrNullSchema = OrSchema.make(recordSchema(), nullSchema());
    return recordOrNullSchema;
  }
  
  public static Schema stringOrNullSchema()
  {
    if (stringOrNullSchema  == null) stringOrNullSchema = OrSchema.make(stringSchema(), nullSchema());
    return stringOrNullSchema;
  }
  
  public static Schema numericOrNullSchema()
  {
    if (numericOrNullSchema  == null) numericOrNullSchema = OrSchema.make(numericSchema(), nullSchema());
    return numericOrNullSchema;
  }

  public static Schema numberOrNullSchema()
  {
    if (numberOrNullSchema  == null) numberOrNullSchema = OrSchema.make(numberSchema(), nullSchema());
    return numberOrNullSchema;
  }

  /** Returns a schema that matches values of the specified classes and null. */
  public static Schema make(Class<? extends JsonValue> ... classes)
  {
    JsonType[] types = new JsonType[classes.length+1];
    for (int i=0; i<classes.length; i++)
    {
      types[i] = JsonType.getType(classes[i]);
      if (types[i] == null) return SchemaFactory.anySchema();
    }
    types[types.length-1] = JsonType.NULL;
    return make(types);
  }
  
  /** Returns a schema for the specified JSON types. */ 
  public static Schema make(JsonType ... types)
  {
    Schema[] schemata = new Schema[types.length];
    for (int i=0; i<types.length; i++)
    {
      schemata[i] = makeInternal(types[i]);
    }
    return OrSchema.make(schemata);
  }

  /** Returns a schema for the specified JSON type. */ 
  private static Schema makeInternal(JsonType type)
  {
    switch (type)
    {
    case BOOLEAN:
      return booleanSchema();
    case LONG:
      return longSchema();
    case DECFLOAT:
      return decfloatSchema();
    case DOUBLE:
      return doubleSchema();
    case STRING:
      return stringSchema();
    case BINARY:
      return binarySchema();
    case DATE:
      return dateSchema();
    case SCHEMA:
      return schematypeSchema();
    case ARRAY:
      return arraySchema();
    case RECORD:
      return recordSchema();
    case NULL:
      return nullSchema();
    case FUNCTION:
      return functionSchema();
    case REGEX:
    case JAVAOBJECT:
    case SPAN:
      return new GenericSchema(type);      
    default:
      return nonNullSchema();
    }
  }
  // -- construction for the parser ---------------------------------------------------------------
  
  /**
   * @param typeConstructorName
   * @return The parameter descriptor for the constructor function with the specified type name.
   */
  public static JsonValueParameters getParameters(String typeConstructorName)
  {
    // TODO: this code should be table-driven. It requires Parameter factory classes.
    if( typeConstructorName.equals("nonnull") )
    {
      return new JsonValueParameters();
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
      return DecfloatSchema.getParameters();
    }
    else if( typeConstructorName.equals("double") )
    {
      return DoubleSchema.getParameters();
    }
    else if( typeConstructorName.equals("function") )
    {
      return FunctionSchema.getParameters();
    }
    else if( typeConstructorName.equals("long") )
    {
      return LongSchema.getParameters();
    }
    else if( typeConstructorName.equals("schematype") )
    {
      return SchematypeSchema.getParameters();
    }
    else if( typeConstructorName.equals("string") )
    {
      return StringSchema.getParameters();
    }
    else if( typeConstructorName.equals("any") )
    {
      return new JsonValueParameters();
    }
    else 
    throw new RuntimeException("undefined type: " + typeConstructorName);
  }

  public static Schema make(String typeConstructorName, JsonRecord args)
  {
    // TODO: this code should be table-driven.
    if( typeConstructorName.equals("nonnull") )
    {
      return SchemaFactory.nonNullSchema();
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
      return new DecfloatSchema(args);
    }
    else if( typeConstructorName.equals("double") )
    {
      return new DoubleSchema(args);
    }
    else if( typeConstructorName.equals("function") )
    {
      return new FunctionSchema(args);
    }
    else if( typeConstructorName.equals("long") )
    {
      return new LongSchema(args);
    }
    else if( typeConstructorName.equals("schematype") )
    {
      return new SchematypeSchema(args);
    }
    else if( typeConstructorName.equals("string") )
    {
      return new StringSchema(args);
    }
    else if( typeConstructorName.equals("any") )
    {
      return SchemaFactory.anySchema();
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
        fields.add(new RecordSchema.Field(name, schemaOf(value), false));
      }
      return new RecordSchema(fields.toArray(new RecordSchema.Field[fields.size()]), null);
      
    case BOOLEAN:
      return new BooleanSchema((JsonBool)v);
      
    case STRING:
      return new StringSchema((JsonString)v);
    case LONG:
      return new LongSchema((JsonLong)v);
    case DOUBLE:
      return new DoubleSchema((JsonDouble)v);
    case DECFLOAT:
      return new DecfloatSchema((JsonDecimal)v);
      
    // JSON extensions

    case BINARY:
      return new BinarySchema((JsonBinary)v);
      
    case DATE:
      return new DateSchema((JsonDate)v);
      
    case SCHEMA:
      return new SchematypeSchema((JsonSchema)v);
    
    case FUNCTION:
      return new FunctionSchema();
      
    case JAVAOBJECT:
    case REGEX:
    default:
      return nonNullSchema();
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
