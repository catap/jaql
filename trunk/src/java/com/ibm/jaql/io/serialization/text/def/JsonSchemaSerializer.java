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
package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.schema.AnyNonNullSchema;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.schema.BooleanSchema;
import com.ibm.jaql.json.schema.DateSchema;
import com.ibm.jaql.json.schema.DecimalSchema;
import com.ibm.jaql.json.schema.DoubleSchema;
import com.ibm.jaql.json.schema.GenericSchema;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.RangeSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Parameters;

public class JsonSchemaSerializer extends TextBasicSerializer<JsonSchema>
{

  @Override
  public void write(PrintStream out, JsonSchema value, int indent)
      throws IOException
  {
    out.print("schema ");
    Schema schema = value.get();
    write(out, schema, indent+7);
  }

  // -- generic write method ----------------------------------------------------------------------
  
  public static void write(PrintStream out, Schema schema, int indent) throws IOException
  {
    switch (schema.getSchemaType())
    {
    case ANY_NON_NULL:
      writeAny(out, (AnyNonNullSchema)schema, indent);
      break;
    case ARRAY:
      writeArray(out, (ArraySchema)schema, indent);
      break;
    case BINARY:
      writeBinary(out, (BinarySchema)schema, indent);
      break;
    case BOOLEAN:
      writeBoolean(out, (BooleanSchema)schema, indent);
      break;
    case DECFLOAT:
      writeDecimal(out, (DecimalSchema)schema, indent);
      break;
    case DATE:
      writeDate(out, (DateSchema)schema, indent);
      break;
    case DOUBLE:
      writeDouble(out, (DoubleSchema)schema, indent);
      break;
    case GENERIC:
      writeGeneric(out, (GenericSchema)schema, indent);
      break;
    case NULL:
      writeNull(out, (NullSchema)schema, indent);
      break;
    case LONG:
      writeLong(out, (LongSchema)schema, indent);
      break;
    case OR:
      writeOr(out, (OrSchema)schema, indent);
      break;
    case RECORD:
      writeRecord(out, (RecordSchema)schema, indent);
      break;
    case STRING:
      writeString(out, (StringSchema)schema, indent);
      break;
    default:
      throw new IllegalStateException("unknown schema type");    
    }
  }
  
  // -- write methods for each schema type --------------------------------------------------------
  
  private static void writeAny(PrintStream out, AnyNonNullSchema schema, int indent) throws IOException
  {
    out.print("any");
  }
  
  private static void writeArray(PrintStream out, ArraySchema schema, int indent) throws IOException
  {
    // handle empty arrays
    if (schema.isEmpty())
    {
      out.print("[]");
      return;
    }
    
    // non-empty
    out.print("[");
    indent += 2;
    String sep="";
    
    // print head
    Schema[] head = schema.getHeadSchemata();
    for (Schema s : head)
    {
      out.println(sep);
      sep=",";
      indent(out, indent);
      
      write(out, s, indent);
    }
    
    // print rest, if any
    if (schema.hasRest())
    {
      out.println(sep);
      sep=",";
      indent(out, indent);

      Schema rest = schema.getRestSchema();
      write(out, rest, indent);
      
      JsonLong minRest = schema.getMinRest();
      JsonLong maxRest = schema.getMaxRest();
      if (!(JsonUtil.equals(minRest, JsonLong.ONE) && JsonUtil.equals(maxRest, JsonLong.ONE))) // == default
      {
//        if (JsonValue.equals(minRest, JsonLong.ZERO) && maxRest==null)
//        {
//          out.print("*");
//        }
//        else if (JsonValue.equals(minRest, JsonLong.ONE) && maxRest==null)
//        {
//          out.print("+");
//        }
//        else
//        {
          out.print('<');
          writeLengthArgs(out, minRest, maxRest, indent, "");
          out.print('>');
//        }        
      }
    }
    
    // done
    out.println();
    indent -= 2;    
    indent(out, indent);
    out.print("]");
  }
  
  private static void writeBinary(PrintStream out, BinarySchema schema, int indent) throws IOException
  {
    out.print("binary");
    
    JsonLong min = schema.getMinLength();
    JsonLong max = schema.getMaxLength();
    writeArgs(out, indent, BinarySchema.getParameters(), new JsonValue[] { min, max } );
  }

  private static void writeBoolean(PrintStream out, BooleanSchema schema, int indent) throws IOException
  {
    out.print("boolean");
    
    JsonValue value = schema.getValue();
    writeArgs(out, indent, BooleanSchema.getParameters(), new JsonValue[] { value } );
  }
  
  private static void writeDecimal(PrintStream out, DecimalSchema schema, int indent) throws IOException
  {
    out.print("decfloat");
    writeRangeArgs(out, indent, DecimalSchema.getParameters(), schema);    
  }

  private static void writeDate(PrintStream out, DateSchema schema, int indent) throws IOException
  {
    out.print("date");
    writeRangeArgs(out, indent, DateSchema.getParameters(), schema);
  }

  private static void writeDouble(PrintStream out, DoubleSchema schema, int indent) throws IOException
  {
    out.print("double");
    writeRangeArgs(out, indent, DoubleSchema.getParameters(), schema);
  }
  
  private static void writeGeneric(PrintStream out, GenericSchema schema, int indent) throws IOException
  {
    out.print(schema.getType().name);
  }
  
  private static void writeLong(PrintStream out, LongSchema schema, int indent) throws IOException
  {
    out.print("long");
    writeRangeArgs(out, indent, LongSchema.getParameters(), schema);
  }

  private static void writeNull(PrintStream out, NullSchema schema, int indent) throws IOException
  {
    out.print("null");
  }

  private static void writeOr(PrintStream out, OrSchema schema, int indent) throws IOException
  {
    Schema[] schemata = schema.getInternal();
    
    // special case: nullable
    if (schemata.length == 2)
    {
      if (schemata[0] instanceof NullSchema)
      {
        write(out, schemata[1], indent);
        if (!(schemata[1] instanceof NullSchema))
        {
          out.print('?'); 
        }
        return;
      }
      
      if (schemata[1] instanceof NullSchema)
      {
        write(out, schemata[0], indent); // != NullSchema
        out.print('?');
        return;
      }
    }
    
    // default
    String separator = "";
    for (Schema s : schemata)
    {
      out.print(separator);
      write(out, s, indent);      
      separator = " | ";
    }
  }
  
  private static void writeRecord(PrintStream out, RecordSchema schema, int indent) throws IOException
  {
    // handle empty fields
    if (schema.isEmpty())
    {
      out.print("{}");
      return;
    }
    
    // non-empty
    out.print("{");
    indent += 2;

    // print declared fields
    String sep="";
    for (RecordSchema.Field f : schema.getFields())
    {
      out.println(sep);
      sep = ",";
      indent(out, indent);
      String name = JsonUtil.printToString(f.getName());
      out.print(name);
      int o = name.length();
      if (f.isOptional())
      {
        out.print('?');
        o++;
      }
      out.print(": ");
      o+=2;
      
      write(out, f.getSchema(), indent+o);
    }
    
    // print rest
    Schema rest = schema.getRest();
    if (rest != null)
    {
      out.println(sep);
      indent(out, indent);
      out.print("*: ");
      write(out, rest, indent+3);
    }
    
    // done
    out.println();
    indent -= 2;
    indent(out, indent);
    out.print("}");
  }
  
  private static void writeString(PrintStream out, StringSchema schema, int indent) throws IOException
  {
    out.print("string");
    JsonLong min = schema.getMinLength();
    JsonLong max = schema.getMaxLength();
    JsonString pattern = schema.getPattern();
    JsonString value = schema.getValue();
    
    writeArgs(out, indent, StringSchema.getParameters(),
        new JsonValue[] { min, max, pattern, value });
  }
  
  // -- helpers -----------------------------------------------------------------------------------
  
  private static void writeArgs(PrintStream out, int indent, 
      Parameters parameters, JsonValue[] values ) throws IOException
  {
    assert values.length == parameters.noParameters();
    String sep="(";
    boolean printed = false;
    for (int i=0; i<values.length; i++)
    {
      
      if (i<parameters.noRequiredParamters())
      {
        out.print(sep);
        JsonUtil.print(out, values[i], indent);
        printed = true;
        sep = ", ";
      }
      else if (!JsonUtil.equals(values[i], parameters.defaultOf(i)))
      {
        out.print(sep);
        out.print(parameters.nameOf(i).toString()); // no quotes
        out.print("=");
        JsonUtil.print(out, values[i], indent);
        printed = true;
        sep = ", ";
      }
    }
    if (printed)
    { 
      out.print(")");
    }
  }
  
  private static <T extends JsonValue> void writeRangeArgs(
      PrintStream out, int indent,  Parameters parameters, RangeSchema<T> schema) 
  throws IOException
  {
    T min = schema.getMin();
    T max = schema.getMax();
    T value = schema.getValue();
    writeArgs(out, indent, parameters, new JsonValue[] { min, max, value });
  }
  
  private static void writeLengthArgs(PrintStream out, JsonNumeric min, JsonNumeric max, 
      int indent, String sep) throws IOException
  {
    out.print(sep);
    sep=",";
    if (min==null)
    {
      out.print('*');
    }
    else
    {
      JsonUtil.print(out, min, indent);
    }

    if (!JsonUtil.equals(min, max))
    {
      out.print(sep);
      if (max==null)
      {
        out.print('*');
      }
      else
      {
        JsonUtil.print(out, max, indent);
      }
    }
  }
}
