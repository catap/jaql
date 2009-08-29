package com.ibm.jaql.io.serialization.binary.temp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.ibm.jaql.io.serialization.FullSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.util.BaseUtil;

class ArraySerializer extends BinaryBasicSerializer<JsonArray>
{
  private ArraySchema schema;
  private BinaryFullSerializer[] headSerializers;
  private BinaryFullSerializer restSerializer;
  Long fixedRestLength; // if ! = null
  
  // -- construction ------------------------------------------------------------------------------
  
  public ArraySerializer(ArraySchema schema)
  {
    this.schema = schema; 
    init();
  }
  
  private void init()
  {
    List<Schema> head = schema.getHeadSchemata();
    headSerializers = new BinaryFullSerializer[head.size()];
    for (int i=0; i<head.size(); i++)
    {
      headSerializers[i] = new TempBinaryFullSerializer(head.get(i));
    }
    
    Schema rest = schema.getRestSchema();
    if (rest != null)
    {
      restSerializer = new TempBinaryFullSerializer(rest);
      if (schema.getMaxRest() != null && schema.getMinRest().get() == schema.getMaxRest().get())
      {
        fixedRestLength = schema.getMinRest().get();
      }
    }
  }

  
  // -- serialization -----------------------------------------------------------------------------
  
  @Override
  public JsonArray read(DataInput in, JsonValue target) throws IOException
  {
    // construct target
    // TODO: make type dependent on schema?
    SpilledJsonArray t;
    if (!(target instanceof SpilledJsonArray))
    {
      t = new SpilledJsonArray();
    }
    else
    {
      t = (SpilledJsonArray)target;
      t.clear();
    }
    
    // read the head
    for (int i=0; i<headSerializers.length; i++)
    {
      t.addCopySerialized(in, headSerializers[i]);
    }
    
    // read the rest
    if (restSerializer != null)
    {
      long n;
      
      // get the count
      if (fixedRestLength == null)
      {
        n = BaseUtil.readVULong(in);
      }
      else
      {
        n = fixedRestLength;
      }

      // read the elements
      for (long i=0; i<n; i++)
      {
        // TODO: use cache here?
        t.addCopySerialized(in, restSerializer);
      }
    }
    
    // done
    return t;
  }

  @Override
  public void write(DataOutput out, JsonArray value) throws IOException
  {
    try
    {
      // check the count
      long n = value.count();
      if (n<schema.minElements().get() || (schema.maxElements() != null && n>schema.maxElements().get()))
      {
        throw new IllegalArgumentException("input array has invalid length");
      }
      
      // get an iterator
      JsonIterator iter = value.iter();
    
      // serialize the head
      for (BinaryFullSerializer headSerializer : headSerializers)
      {
        boolean success = iter.moveNext();
        assert success; // guaranteed by checking count above 
        headSerializer.write(out, iter.current());
      }
      n -= headSerializers.length;
      
      // serialize the rest
      if (restSerializer != null)
      {
        // serialize the count
        if (fixedRestLength == null)
        {
          BaseUtil.writeVULong(out, n);
        }

        // write the elements
        for (long i=0; i<n; i++)
        {
          boolean success = iter.moveNext();
          assert success; // guaranteed by checking count above 
          restSerializer.write(out, iter.current());
        }        
      }
    } 
    catch (Exception e)
    {
      throw new IOException(e);
    }
  }
  

  // -- comparison --------------------------------------------------------------------------------

  public int compare(DataInput in1, DataInput in2) throws IOException {
    // compare the head
    for (BinaryFullSerializer headSerializer : headSerializers)
    {
      int cmp = headSerializer.compare(in1, in2);
      if (cmp != 0) return cmp;
    }
    
    // compare tails
    if (restSerializer != null)
    {
      // deserialize the count
      long n1, n2;
      
      // get the count
      if (fixedRestLength == null)
      {
        n1 = BaseUtil.readVULong(in1);
        n2 = BaseUtil.readVULong(in2);
      }
      else
      {
        n1 = n2 = fixedRestLength;
      }

      return FullSerializer.compareArrays(in1, n1, in2, n2, restSerializer);
    }
    
    // else they are identical
    return 0;
  }
  
  //TODO: efficient implementation of skip, and copy
}
