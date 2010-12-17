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

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.AtomSchema;
import com.ibm.jaql.json.schema.AtomSchemaWithLength;
import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.schema.BooleanSchema;
import com.ibm.jaql.json.schema.DateSchema;
import com.ibm.jaql.json.schema.DecfloatSchema;
import com.ibm.jaql.json.schema.DoubleSchema;
import com.ibm.jaql.json.schema.FunctionSchema;
import com.ibm.jaql.json.schema.GenericSchema;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.schema.NonNullSchema;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaType;
import com.ibm.jaql.json.schema.SchematypeSchema;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.util.FastPrinter;

public class SchemaSerializer extends TextBasicSerializer<JsonSchema>
{
  @Override
  public void write(FastPrinter out, JsonSchema value, int indent)
      throws IOException
  {
    out.print("schema ");
    Schema schema = value.get();
    write(out, schema, indent+7);
  }

  // -- generic write method ----------------------------------------------------------------------
  
  public static void write(FastPrinter out, Schema schema, int indent) throws IOException
  {
    switch (schema.getSchemaType())
    {
    case NON_NULL:
      writeNonNull(out, (NonNullSchema)schema, indent);
      break;
    case ARRAY:
      writeArray(out, (ArraySchema)schema, indent);
      break;
    case SCHEMATYPE:
      writeSchema(out, (SchematypeSchema)schema, indent);
      break;
    case BINARY:
      writeBinary(out, (BinarySchema)schema, indent);
      break;
    case BOOLEAN:
      writeBoolean(out, (BooleanSchema)schema, indent);
      break;
    case DECFLOAT:
      writeDecimal(out, (DecfloatSchema)schema, indent);
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
    case FUNCTION:
      writeFunction(out, (FunctionSchema)schema, indent);
      break;
    default:
      throw new IllegalStateException("unknown schema type");    
    }
  }
  
  // -- write methods for each schema type --------------------------------------------------------
  
  private static void writeNonNull(FastPrinter out, NonNullSchema schema, int indent) throws IOException
  {
    out.print("nonnull");
  }
  
  private static void writeArray(FastPrinter out, ArraySchema schema, int indent) throws IOException
  {
    // handle empty arrays
    if (schema.isEmpty(JsonType.ARRAY).always())
    {
      out.print("[]");
      return;
    }
    
    // handle any array
    if (schema.getHeadSchemata().size() == 0 && schema.hasRest() 
        && schema.getRestSchema() == SchemaFactory.anySchema()) {
      out.print("[ * ]");
      return;
    }
    
    // non-empty
    out.print("[");
    indent += 2;
    String sep="";
    
    // print head
    for (Schema s : schema.getHeadSchemata())
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
      out.print(" * ");
    }
    
    // done
    out.println();
    indent -= 2;    
    indent(out, indent);
    out.print("]");
  }
  
  private static void writeSchema(FastPrinter out, SchematypeSchema schema, int indent) throws IOException
  {
    out.print("schematype");
    writeAtomSchemaArgs(out, indent, schema);
  }
  
  private static void writeBinary(FastPrinter out, BinarySchema schema, int indent) throws IOException
  {
    out.print("binary");
    writeAtomSchemaWithLengthArgs(out, indent, schema);
  }

  private static void writeBoolean(FastPrinter out, BooleanSchema schema, int indent) throws IOException
  {
    out.print("boolean");
    writeAtomSchemaArgs(out, indent, schema);
  }
  
  private static void writeDecimal(FastPrinter out, DecfloatSchema schema, int indent) throws IOException
  {
    out.print("decfloat");
    writeAtomSchemaArgs(out, indent, schema);
  }

  private static void writeDate(FastPrinter out, DateSchema schema, int indent) throws IOException
  {
    out.print("date");
    writeAtomSchemaArgs(out, indent, schema);
  }

  private static void writeDouble(FastPrinter out, DoubleSchema schema, int indent) throws IOException
  {
    out.print("double");
    writeAtomSchemaArgs(out, indent, schema);
  }
  
  private static void writeGeneric(FastPrinter out, GenericSchema schema, int indent) throws IOException
  {
    out.print(schema.getType().toString());
  }
  
  private static void writeFunction(FastPrinter out, FunctionSchema schema, int indent) throws IOException
  {
    out.print("function");
  }
  
  private static void writeLong(FastPrinter out, LongSchema schema, int indent) throws IOException
  {
    out.print("long");
    writeAtomSchemaArgs(out, indent, schema);
  }

  private static void writeNull(FastPrinter out, NullSchema schema, int indent) throws IOException
  {
    out.print("null");
  }

  private static void writeOr(FastPrinter out, OrSchema schema, int indent) throws IOException
  {
    // handle nulls
    boolean matchesNull = false;
    boolean matchesNonNull = false;
    String separator = "";
    int n = 0;
    for (Schema s : schema.get())
    {
      if (s.getSchemaType() == SchemaType.NULL)
      {
        matchesNull = true;
      }
      else if (s.getSchemaType() == SchemaType.NON_NULL)
      {
        matchesNonNull = true;
      }
      else
      {
        out.print(separator);
        write(out, s, indent);      
        n++;
        separator = " | ";
      }
    }
    
    // deal with null and nonnull
    if (matchesNull || matchesNonNull)
    {
      if (matchesNull && matchesNonNull)
      {
        out.print(separator);
        out.print("any");
      }
      else if (n==1 && matchesNull) // but not nonnull
      {
        out.print("?");
      } 
      else
      {
        out.print(separator);
        out.print(matchesNull ? "null" : "nonnull");
      }
    }
  }
  
  private static void writeRecord(FastPrinter out, RecordSchema schema, int indent) throws IOException
  {
    // handle empty fields
    if (schema.isEmpty(JsonType.RECORD).always())
    {
      out.print("{}");
      return;
    }
    
    // handle any record
    if (schema.noRequiredOrOptional() == 0 && schema.hasAdditional() 
        && schema.getAdditionalSchema() == SchemaFactory.anySchema()) {
      out.print("{ * }");
      return;
    }
    
    // non-empty
    out.print("{");
    indent += 2;

    // print declared fields
    String sep="";
    for (RecordSchema.Field f : schema.getFieldsByPosition())
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
      if (f.getSchema() != SchemaFactory.anySchema()) {
        out.print(": ");
        o+=2;
      
        write(out, f.getSchema(), indent+o);
      }
    }
    
    // print rest
    Schema rest = schema.getAdditionalSchema();
    if (rest != null)
    {
      out.println(sep);
      indent(out, indent);
      out.print("*");
      if (rest != SchemaFactory.anySchema()) {
        out.print(": ");
        write(out, rest, indent+3);
      }
    }
    
    // done
    out.println();
    indent -= 2;
    indent(out, indent);
    out.print("}");
  }
  
  private static void writeString(FastPrinter out, StringSchema schema, int indent) throws IOException
  {
    out.print("string");
    writeAtomSchemaWithLengthArgs(out, indent, schema);
  }
  
  // -- helpers -----------------------------------------------------------------------------------
  
  private static void writeArgs(FastPrinter out, int indent, 
      JsonValueParameters parameters, JsonValue[] values ) throws IOException
  {
    assert values.length == parameters.numParameters();
    String sep="(";
    boolean printed = false;
    for (int i=0; i<values.length; i++)
    {
      
      if (i<parameters.numRequiredParameters())
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
  
  private static void writeAtomSchemaArgs(FastPrinter out, int indent, 
      AtomSchema<?> schema) throws IOException
  {
    writeArgs(out, indent, AtomSchema.DEFAULT_PARAMETERS, 
        new JsonValue[] { schema.getConstant(), schema.getAnnotation() });
  }
  
  private static void writeAtomSchemaWithLengthArgs(FastPrinter out, int indent, 
      AtomSchemaWithLength<?> schema) throws IOException
  {
    writeArgs(out, indent, AtomSchemaWithLength.DEFAULT_PARAMETERS, 
        new JsonValue[] { schema.getLength(), schema.getConstant(), schema.getAnnotation() });
  }

}
