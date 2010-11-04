package com.ibm.jaql.io.serialization.text.schema;

import java.io.IOException;
import java.util.List;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.FastPrinter;

final class ArraySerializer extends TextBasicSerializer<JsonArray>
{
  private ArraySchema schema;
  private TextFullSerializer[] headSerializers;
  private TextFullSerializer restSerializer;
  
  // -- construction ------------------------------------------------------------------------------
  
  public ArraySerializer(ArraySchema schema)
  {
    this.schema = schema; 
    init();
  }
  
  private void init()
  {
    List<Schema> head = schema.getHeadSchemata();
    headSerializers = new TextFullSerializer[head.size()];
    for (int i=0; i<head.size(); i++)
    {
      headSerializers[i] = new SchemaTextFullSerializer(head.get(i));
    }
    
    Schema rest = schema.getRestSchema();
    if (rest != null)
    {
      restSerializer = new SchemaTextFullSerializer(rest);
    }
  }

  
  // -- serialization -----------------------------------------------------------------------------
  
  @Override
  public void write(FastPrinter out, JsonArray value, int indent) throws IOException
  {
    try
    {
      // check the count
      long n = value.count();
      if (n<schema.minElements().get() || (schema.maxElements() != null && n>schema.maxElements().get()))
      {
        throw new IllegalArgumentException("input array has invalid length");
      }
      
      // print header
      String sep = "";
      out.print("[");
      indent += 2;

      // get an iterator
      JsonIterator iter = value.iter();
    
      // serialize the head
      for (TextFullSerializer headSerializer : headSerializers)
      {
        boolean success = iter.moveNext();
        assert success; // guaranteed by checking count above 
        out.println(sep);
        indent(out, indent);
        headSerializer.write(out, iter.current(), indent);
        sep = ",";        
      }
      n -= headSerializers.length;
      
      // serialize the rest
      if (restSerializer != null)
      {
        // write the elements
        for (long i=0; i<n; i++)
        {
          boolean success = iter.moveNext();
          assert success; // guaranteed by checking count above 
          out.println(sep);
          indent(out, indent);
          restSerializer.write(out, iter.current(), indent);
          sep = ",";
        }        
      }
      
      // print footer
      indent -= 2;
      if (sep.length() > 0) // if not empty array
      {
        out.println();
        indent(out, indent);
      }
      out.print("]");
    } 
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }
}
