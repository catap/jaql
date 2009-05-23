package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.schema.AnySchema;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.schema.DecimalSchema;
import com.ibm.jaql.json.schema.DoubleSchema;
import com.ibm.jaql.json.schema.GenericSchema;
import com.ibm.jaql.json.schema.NullSchema;
import com.ibm.jaql.json.schema.LongSchema;
import com.ibm.jaql.json.schema.NumericSchema;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.StringSchema;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JsonSchemaSerializer extends TextBasicSerializer<JsonSchema>
{

  @Override
  public void write(PrintStream out, JsonSchema value, int indent)
      throws IOException
  {
    out.print("schema ");
    Schema schema = value.getSchema();
    write(out, schema, indent+7);
  }

  // -- generic write method ----------------------------------------------------------------------
  
  public static void write(PrintStream out, Schema schema, int indent) throws IOException
  {
    switch (schema.getSchemaType())
    {
    case ANY:
      writeAny(out, (AnySchema)schema, indent);
      break;
    case ARRAY:
      writeArray(out, (ArraySchema)schema, indent);
      break;
    case BINARY:
      writeBinary(out, (BinarySchema)schema, indent);
      break;
    case DECIMAL:
      writeDecimal(out, (DecimalSchema)schema, indent);
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
  
  private static void writeAny(PrintStream out, AnySchema schema, int indent) throws IOException
  {
    out.print('*');
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
    Schema[] head = schema.getInternalHead();
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

      Schema rest = schema.getInternalRest();
      write(out, rest, indent);
      
      JsonLong minRest = schema.getMinRest();
      JsonLong maxRest = schema.getMaxRest();
      if (!(JsonValue.equals(minRest, JsonLong.ONE) && JsonValue.equals(maxRest, JsonLong.ONE))) // == default
      {
        if (JsonValue.equals(minRest, JsonLong.ZERO) && maxRest==null)
        {
          out.print("*");
        }
        else if (JsonValue.equals(minRest, JsonLong.ONE) && maxRest==null)
        {
          out.print("+");
        }
        else
        {
          out.print('<');
          writeLengthArgs(out, minRest, maxRest, indent, "");
          out.print('>');
        }        
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
    if (min != null || max != null) {
      out.write('(');
      writeLengthArgs(out, min, max, indent, "");
      out.write(')');
    }
  }

  private static void writeDecimal(PrintStream out, DecimalSchema schema, int indent) throws IOException
  {
    out.print("decimal");
    writeNumericArgs(out, schema, indent);
  }
  
  private static void writeDouble(PrintStream out, DoubleSchema schema, int indent) throws IOException
  {
    out.print("double");
    writeNumericArgs(out, schema, indent);
  }
  
  private static void writeGeneric(PrintStream out, GenericSchema schema, int indent) throws IOException
  {
    out.print(schema.getType().name);
  }
  
  private static void writeLong(PrintStream out, LongSchema schema, int indent) throws IOException
  {
    out.print("long");
    writeNumericArgs(out, schema, indent);
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
  
  
  // -- helpers -----------------------------------------------------------------------------------
  
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
      String name = JsonValue.printToString(f.getName());
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
    
    JsonString pattern = schema.getPattern();
    JsonLong min = schema.getMinLength();
    JsonLong max = schema.getMaxLength();
    if (pattern!=null || min!=null || max!=null)
    {
      out.print('(');
      
      String sep="";
      if (pattern!=null)
      {
        JsonValue.print(out, schema.getPattern());
        sep=", ";
      }
      
      if (min != null || max != null) {
        writeLengthArgs(out, min, max, indent, sep);        
      }
      
      out.print(')');
    }
  }
  
  // -- helpers -----------------------------------------------------------------------------------
  
  private static void writeLengthArgs(PrintStream out, JsonNumeric min, JsonNumeric max, 
      int indent, String sep) throws IOException
  {
    out.print(sep);
    sep=", ";
    if (min==null)
    {
      out.print('*');
    }
    else
    {
      JsonValue.print(out, min, indent);
    }

    if (!JsonValue.equals(min, max))
    {
      out.print(sep);
      if (max==null)
      {
        out.print('*');
      }
      else
      {
        JsonValue.print(out, max, indent);
      }
    }
  }
  
  private static void writeNumericArgs(PrintStream out, NumericSchema<?> schema, int indent) throws IOException
  {
    JsonNumeric min = schema.getMin();
    JsonNumeric max = schema.getMax();
    if (!(min==null && max==null))
    {
      out.print("(");

      if (min==null) // max != null
      {
         out.print("*,");
         JsonValue.print(out, max, indent);
      } else if (max==null) { // min != null
        JsonValue.print(out, min, indent);
        out.print(",*");        
      } else { // both != null
        JsonValue.print(out, min, indent);
        if (!min.equals(max))
        {
          out.print(", ");        
          JsonValue.print(out, max, indent);
        }
      }
      
      out.print(")");
    }
  }

}
